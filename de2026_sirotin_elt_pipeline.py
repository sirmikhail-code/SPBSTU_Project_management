from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import logging

"""
Комментарии.
1) В коде предусмотрена принудительная конвертация типа money в тип numeric, а именно для данных из таблиц public.deal.
2) Для слоя витрины (схема public в БД DE2026_Sirotin) предусмотрена инкрементальная загрузка, а именно:
таск load_dim_operation_type — использует WHERE id NOT IN (дедупликация, но не обновление)
таск load_dim_subdivision — аналогично
таск load_fact_income_forecast — использует WHERE NOT EXISTS + ON CONFLICT
Была мысль сделать обновление уже имеющихся записей, но не совсем понятно на каких исходных данных его строить. 
"""

# Настройка логирования
logger = logging.getLogger(__name__)

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'sirotin',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определяем DAG
dag = DAG(
    dag_id='de2026_sirotin_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline: загрузка из CRM и 1С в staging и витрины',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['staging', 'dwh', 'sirotin'],
)

# ============================================
# 1. Подготовительные задачи
# ============================================

start_pipeline = DummyOperator(
    task_id='start_elt_pipeline',
    dag=dag,
)

end_staging_phase = DummyOperator(
    task_id='end_staging_phase',
    dag=dag,
)

end_pipeline = DummyOperator(
    task_id='end_elt_pipeline',
    dag=dag,
)

# ============================================
# 2. Очистка staging таблиц перед загрузкой
# ============================================

truncate_stg_crm_income = PostgresOperator(
    task_id='truncate_stg_crm_income_forecast',
    postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin',
    sql="TRUNCATE TABLE staging.stg_crm_income_forecast;",
    dag=dag,
)

truncate_stg_subdivision = PostgresOperator(
    task_id='truncate_stg_1c_subdivision',
    postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin',
    sql="TRUNCATE TABLE staging.stg_1c_subdivision;",
    dag=dag,
)

truncate_stg_operation_type = PostgresOperator(
    task_id='truncate_stg_crm_operation_type',
    postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin',
    sql="TRUNCATE TABLE staging.stg_crm_operation_type;",
    dag=dag,
)

# ============================================
# 3. Функции для загрузки данных
# ============================================

def load_crm_income_from_msk(**context):
    """
    Загружает прогнозные доходы из CRM Москва в staging.stg_crm_income_forecast
    """
    source_hook = PostgresHook(postgres_conn_id='postgres_for_ProjectManagement_DE2026_MscDB')
    target_hook = PostgresHook(postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin')
    
    logger.info("Начало загрузки данных из CRM Москва")
    
    extract_sql = """
        SELECT 
            id,
            plan_service_date,
            service_id,
            amount::numeric,
            status
        FROM public.deal
        WHERE (status IN (1, 2) OR status IS NULL)
          AND plan_service_date > '2023-07-31'::DATE
    """
    
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute(extract_sql)
    rows = source_cursor.fetchall()
    source_cursor.close()
    
    logger.info(f"Получено {len(rows)} строк из CRM Москва")
    
    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()
    
    insert_sql = """
        INSERT INTO staging.stg_crm_income_forecast (
            id, plan_service_date, subdivision_id, region_id,
            income_outcome_id, service_id, amount, status,
            svc_loading_datetime, source_system
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    inserted_count = 0
    for row in rows:
        try:
            target_cursor.execute(insert_sql, (
                row[0],  # id
                row[1],  # plan_service_date
                1,       # subdivision_id (Москва)
                1,       # region_id (Москва)
                1,       # income_outcome_id (доход)
                row[2],  # service_id
                row[3],  # amount
                row[4],  # status
                '2023-07-31',
                'MskDB'
            ))
            inserted_count += 1
        except Exception as e:
            logger.error(f"Ошибка при вставке строки id={row[0]}: {e}")
    
    target_conn.commit()
    target_cursor.close()
    target_conn.close()
    
    logger.info(f"Вставлено {inserted_count} строк в staging.stg_crm_income_forecast из CRM Москва")
    return inserted_count

def load_crm_income_from_spb(**context):
    """
    Загружает прогнозные доходы из CRM Санкт-Петербург в staging.stg_crm_income_forecast
    """
    source_hook = PostgresHook(postgres_conn_id='postgres_for_ProjectManagement_DE2026_SPbDB')
    target_hook = PostgresHook(postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin')
    
    logger.info("Начало загрузки данных из CRM Санкт-Петербург")
    
    extract_sql = """
        SELECT 
            id,
            plan_service_date,
            service_id,
            amount::numeric,
            status
        FROM public.deal
        WHERE (status IN (1, 2) OR status IS NULL)
          AND plan_service_date > '2023-07-31'::DATE
    """
    
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute(extract_sql)
    rows = source_cursor.fetchall()
    source_cursor.close()
    
    logger.info(f"Получено {len(rows)} строк из CRM Санкт-Петербург")
    
    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()
    
    insert_sql = """
        INSERT INTO staging.stg_crm_income_forecast (
            id, plan_service_date, subdivision_id, region_id,
            income_outcome_id, service_id, amount, status,
            svc_loading_datetime, source_system
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    inserted_count = 0
    for row in rows:
        try:
            target_cursor.execute(insert_sql, (
                row[0],  # id
                row[1],  # plan_service_date
                2,       # subdivision_id (Санкт-Петербург)
                2,       # region_id (Санкт-Петербург)
                1,       # income_outcome_id (доход)
                row[2],  # service_id
                row[3],  # amount
                row[4],  # status
                '2023-07-31',
                'SpbDB'
            ))
            inserted_count += 1
        except Exception as e:
            logger.error(f"Ошибка при вставке строки id={row[0]}: {e}")
    
    target_conn.commit()
    target_cursor.close()
    target_conn.close()
    
    logger.info(f"Вставлено {inserted_count} строк в staging.stg_crm_income_forecast из CRM Санкт-Петербург")
    return inserted_count

def load_1c_subdivisions(**context):
    """
    Загружает справочник подразделений из 1С в staging.stg_1c_subdivision
    """
    source_hook = PostgresHook(postgres_conn_id='postgres_for_ProjectManagement_DE2026_1c_db')
    target_hook = PostgresHook(postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin')
    
    logger.info("Начало загрузки подразделений из 1С")
    
    extract_sql = """
        SELECT DISTINCT category
        FROM reporting.expense
        WHERE category IS NOT NULL
    """
    
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute(extract_sql)
    rows = source_cursor.fetchall()
    source_cursor.close()
    
    logger.info(f"Получено {len(rows)} уникальных категорий из 1С")
    
    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()
    
    insert_sql = """
        INSERT INTO staging.stg_1c_subdivision (subdivision_id, subdivision_name, source_system)
        VALUES (%s, %s, %s)
    """
    
    inserted_count = 0
    for row in rows:
        category = row[0]
        
        if 'Бек' in category or 'бэк' in category:
            sub_id, sub_name = 3, 'Бэк-офис'
        elif 'Москв' in category:
            sub_id, sub_name = 1, 'Продажи Мск'
        elif 'Санкт' in category:
            sub_id, sub_name = 2, 'Продажи СПб'
        else:
            logger.warning(f"Неизвестная категория: {category} — пропускаем")
            continue
        
        try:
            target_cursor.execute(insert_sql, (sub_id, sub_name, '1C_Expense'))
            inserted_count += 1
        except Exception as e:
            logger.error(f"Ошибка при вставке категории {category}: {e}")
    
    target_conn.commit()
    target_cursor.close()
    target_conn.close()
    
    logger.info(f"Загружено {inserted_count} подразделений в staging.stg_1c_subdivision")
    return inserted_count

def load_crm_operation_types(**context):
    """
    Загружает справочник типов операций из CRM в staging.stg_crm_operation_type
    """
    target_hook = PostgresHook(postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin')
    
    logger.info("Начало загрузки типов операций из CRM")
    
    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()
    
    insert_sql = """
        INSERT INTO staging.stg_crm_operation_type (operation_type_id, operation_name, source_system)
        VALUES (%s, %s, %s)
        ON CONFLICT (operation_type_id) DO NOTHING
    """
    
    inserted_count = 0
    sources = [
        ('postgres_for_ProjectManagement_DE2026_MscDB', 'MskDB'),
        ('postgres_for_ProjectManagement_DE2026_SPbDB', 'SpbDB')
    ]
    
    for conn_id, source_name in sources:
        source_hook = PostgresHook(postgres_conn_id=conn_id)
        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
        
        source_cursor.execute("SELECT id, name FROM public.service WHERE name IS NOT NULL")
        rows = source_cursor.fetchall()
        source_cursor.close()
        
        logger.info(f"Получено {len(rows)} типов операций из {source_name}")
        
        for row in rows:
            target_cursor.execute(insert_sql, (row[0], row[1], source_name))
            if target_cursor.rowcount > 0:
                inserted_count += 1
        
        source_conn.close()
    
    target_conn.commit()
    target_cursor.close()
    target_conn.close()
    
    logger.info(f"Загружено {inserted_count} типов операций в staging.stg_crm_operation_type")
    return inserted_count

# ============================================
# 4. Операторы загрузки в staging
# ============================================

load_crm_msk = PythonOperator(
    task_id='load_crm_income_from_msk',
    python_callable=load_crm_income_from_msk,
    dag=dag,
)

load_crm_spb = PythonOperator(
    task_id='load_crm_income_from_spb',
    python_callable=load_crm_income_from_spb,
    dag=dag,
)

load_1c_subdivisions_op = PythonOperator(
    task_id='load_1c_subdivisions',
    python_callable=load_1c_subdivisions,
    dag=dag,
)

load_crm_operation_types_op = PythonOperator(
    task_id='load_crm_operation_types',
    python_callable=load_crm_operation_types,
    dag=dag,
)

# ============================================
# 5. Загрузка из staging в витрину (Dimension Tables)
# ============================================

load_dim_operation_type = PostgresOperator(
    task_id='load_dim_operation_type',
    postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin',
    sql="""
        INSERT INTO dim_operation_type (id, name)
        SELECT DISTINCT
            operation_type_id,
            operation_name
        FROM staging.stg_crm_operation_type
        WHERE operation_type_id NOT IN (SELECT id FROM dim_operation_type)
        ON CONFLICT (id) DO NOTHING;
    """,
    dag=dag,
)

load_dim_subdivision = PostgresOperator(
    task_id='load_dim_subdivision',
    postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin',
    sql="""
        INSERT INTO dim_subdivision (id, name)
        SELECT 
            subdivision_id,
            subdivision_name
        FROM staging.stg_1c_subdivision
        WHERE subdivision_id NOT IN (SELECT id FROM dim_subdivision)
        ON CONFLICT (id) DO NOTHING;
    """,
    dag=dag,
)

# ============================================
# 6. Загрузка из staging в витрину (Fact Table)
# ============================================

load_fact_income_forecast = PostgresOperator(
    task_id='load_fact_income_forecast',
    postgres_conn_id='postgres_for_ProjectManagement_DE2026_Sirotin',
    sql="""
        INSERT INTO fact_income_forecast (
            forecast_date_id,
            subdivision_id,
            region_id,
            income_outcome_id,
            service_id,
            amount
        )
        SELECT 
            dc.id AS forecast_date_id,
            s.subdivision_id,
            s.region_id,
            s.income_outcome_id,
            s.service_id,
            s.amount
        FROM staging.stg_crm_income_forecast s
        LEFT JOIN dim_calendar dc ON dc.actual_date = s.plan_service_date
        WHERE NOT EXISTS (
            SELECT 1 FROM fact_income_forecast f
            WHERE f.forecast_date_id = dc.id
              AND f.subdivision_id = s.subdivision_id
              AND f.region_id = s.region_id
              AND f.service_id = s.service_id
        );
    """,
    dag=dag,
)

# ============================================
# 7. Зависимости (граф выполнения)
# ============================================

start_pipeline >> [truncate_stg_crm_income, truncate_stg_subdivision, truncate_stg_operation_type]

# От каждой truncate задачи к соответствующим load задачам
truncate_stg_crm_income >> [load_crm_msk, load_crm_spb]
truncate_stg_subdivision >> load_1c_subdivisions_op
truncate_stg_operation_type >> load_crm_operation_types_op

# Все load задачи staging перед завершением staging фазы
[load_crm_msk, load_crm_spb, load_1c_subdivisions_op, load_crm_operation_types_op] >> end_staging_phase

# Загрузка в витрину после staging
end_staging_phase >> [load_dim_operation_type, load_dim_subdivision] >> load_fact_income_forecast >> end_pipeline