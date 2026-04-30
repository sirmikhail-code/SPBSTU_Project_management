/*
 1)Ввиду того, что DBeaver не позволяет запустить и создание БД, и создание таблиц в ней "одним махом", необходимо сначала создать БД DE2026_Sirotin (Блок 1. СОздание БД DE2026_Sirotin),
 затем создать соединение к этой БД,
 затем переподключиться через вновь созданное соединение (к этой новой БД) 
 и только после этого запускать остальную часть скрипта  (Блок 2. Создание таблиц и индексов в БД DE2026_Sirotin).
 Гугл подсказывает, что более расширенным функционалом обладает psql (консольный клиент PostgreSQL) и из него можно такие вещи запускать сразу.
 2)Добавил комменты COMMENT ON TABLE и COMMENT ON COLUMN, т.к. удобно при наведении на сущность увидеть во всплывающем окне комментарий о ней. Не нужно лазить через контекстное меню 
 в описание (как в MS SQL SERVER например).
 */

/************************************************************** Блок 1. СОздание БД DE2026_Sirotin ****************************************************************************/

-- =====================================================
-- 1. СОЗДАНИЕ БАЗЫ ДАННЫХ
-- =====================================================

DROP DATABASE IF EXISTS "DE2026_Sirotin";
CREATE DATABASE "DE2026_Sirotin"
    ENCODING = 'UTF8'

/************************************************************** Блок 2. Создание таблиц и индексов в БД DE2026_Sirotin *********************************************************/    
  
-- =====================================================
-- 2. ТАБЛИЦЫ ИЗМЕРЕНИЙ (СПРАВОЧНИКИ)
-- =====================================================

-- 2.1 Календарь (dim_calendar)
DROP TABLE IF exists "dim_calendar" CASCADE;
CREATE TABLE dim_calendar (
    id          SERIAL PRIMARY KEY,
    actual_date DATE NOT NULL UNIQUE,
    month       INTEGER NOT NULL,
    quarter     INTEGER NOT NULL,
    year        INTEGER NOT NULL
);

COMMENT ON TABLE dim_calendar IS 'Справочник дат (календарь)';
COMMENT ON COLUMN dim_calendar.id IS 'Суррогатный первичный ключ';
COMMENT ON COLUMN dim_calendar.actual_date IS 'Фактическая дата';
COMMENT ON COLUMN dim_calendar.month IS 'Номер месяца (1-12)';
COMMENT ON COLUMN dim_calendar.quarter IS 'Номер квартала (1-4)';
COMMENT ON COLUMN dim_calendar.year IS 'Год';

-- 2.2 Подразделения (dim_subdivision)
DROP TABLE IF exists "dim_subdivision" CASCADE;
CREATE TABLE dim_subdivision (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_subdivision IS 'Справочник подразделений компании';
COMMENT ON COLUMN dim_subdivision.id IS 'Суррогатный первичный ключ';
COMMENT ON COLUMN dim_subdivision.name IS 'Название подразделения';

-- 2.3 Регионы (dim_region)
DROP TABLE IF exists "dim_region" CASCADE;
CREATE TABLE dim_region (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_region IS 'Справочник регионов';
COMMENT ON COLUMN dim_region.id IS 'Суррогатный первичный ключ';
COMMENT ON COLUMN dim_region.name IS 'Название региона';

-- 2.4 Доходы/расходы (dim_income_outcome)
DROP TABLE IF exists "dim_income_outcome" CASCADE;
CREATE TABLE dim_income_outcome (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_income_outcome IS 'Тип движения денег (доход/расход)';
COMMENT ON COLUMN dim_income_outcome.id IS 'Суррогатный первичный ключ';
COMMENT ON COLUMN dim_income_outcome.name IS 'Наименование (например, "Доход", "Расход")';

-- 2.5 Типы операций (dim_operation_type)
DROP TABLE IF exists "dim_operation_type" CASCADE;
CREATE TABLE dim_operation_type (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_operation_type IS 'Справочник типов операций/услуг';
COMMENT ON COLUMN dim_operation_type.id IS 'Суррогатный первичный ключ';
COMMENT ON COLUMN dim_operation_type.name IS 'Название типа операции (услуги)';

-- =====================================================
-- 3. ТАБЛИЦЫ ФАКТОВ
-- =====================================================

-- 3.1 Прогнозные доходы (fact_income_forecast
DROP TABLE IF exists "fact_income_forecast" CASCADE;
CREATE TABLE fact_income_forecast (
    id                 SERIAL PRIMARY KEY,
    forecast_date_id   INTEGER NOT NULL,
    subdivision_id     INTEGER NOT NULL,
    region_id          INTEGER NOT NULL,
    income_outcome_id  INTEGER NOT NULL,
    service_id         INTEGER NOT NULL,
    amount             NUMERIC(15, 2) NOT NULL,
    
    -- Внешние ключи
    CONSTRAINT fk_income_forecast_calendar 
        FOREIGN KEY (forecast_date_id) REFERENCES dim_calendar(id),
    CONSTRAINT fk_income_forecast_subdivision 
        FOREIGN KEY (subdivision_id) REFERENCES dim_subdivision(id),
    CONSTRAINT fk_income_forecast_region 
        FOREIGN KEY (region_id) REFERENCES dim_region(id),
    CONSTRAINT fk_income_forecast_income_outcome 
        FOREIGN KEY (income_outcome_id) REFERENCES dim_income_outcome(id),
    CONSTRAINT fk_income_forecast_service 
        FOREIGN KEY (service_id) REFERENCES dim_operation_type(id)
);

COMMENT ON TABLE fact_income_forecast IS 'Факт-таблица прогнозных доходов';
COMMENT ON COLUMN fact_income_forecast.id IS 'Суррогатный первичный ключ';
COMMENT ON COLUMN fact_income_forecast.forecast_date_id IS 'Внешний ключ на дату прогноза (Dim_Calendar)';
COMMENT ON COLUMN fact_income_forecast.subdivision_id IS 'Внешний ключ на подразделение (Dim_Subdivision)';
COMMENT ON COLUMN fact_income_forecast.region_id IS 'Внешний ключ на регион (Dim_Region)';
COMMENT ON COLUMN fact_income_forecast.income_outcome_id IS 'Внешний ключ на тип (Dim_IncomeOutcome)';
COMMENT ON COLUMN fact_income_forecast.service_id IS 'Внешний ключ на услугу (Dim_OperationType)';
COMMENT ON COLUMN fact_income_forecast.amount IS 'Прогнозная сумма';

-- 3.2 Прогнозные расходы (fact_outcome_forecast)
DROP TABLE IF exists "fact_outcome_forecast" CASCADE;
CREATE TABLE fact_outcome_forecast (
    id                 SERIAL PRIMARY KEY,
    forecast_date_id   INTEGER NOT NULL,
    subdivision_id     INTEGER NOT NULL,
    region_id          INTEGER NOT NULL,
    income_outcome_id  INTEGER NOT NULL,
    service_id         INTEGER NOT NULL,
    amount             NUMERIC(15, 2) NOT NULL,
    
    -- Внешние ключи
    CONSTRAINT fk_outcome_forecast_calendar 
        FOREIGN KEY (forecast_date_id) REFERENCES dim_calendar(id),
    CONSTRAINT fk_outcome_forecast_subdivision 
        FOREIGN KEY (subdivision_id) REFERENCES dim_subdivision(id),
    CONSTRAINT fk_outcome_forecast_region 
        FOREIGN KEY (region_id) REFERENCES dim_region(id),
    CONSTRAINT fk_outcome_forecast_income_outcome 
        FOREIGN KEY (income_outcome_id) REFERENCES dim_income_outcome(id),
    CONSTRAINT fk_outcome_forecast_service 
        FOREIGN KEY (service_id) REFERENCES dim_operation_type(id)
);

COMMENT ON TABLE fact_outcome_forecast IS 'Факт-таблица прогнозных расходов';
COMMENT ON COLUMN fact_outcome_forecast.id IS 'Суррогатный первичный ключ';
COMMENT ON COLUMN fact_outcome_forecast.forecast_date_id IS 'Внешний ключ на дату прогноза (Dim_Calendar)';
COMMENT ON COLUMN fact_outcome_forecast.subdivision_id IS 'Внешний ключ на подразделение (Dim_Subdivision)';
COMMENT ON COLUMN fact_outcome_forecast.region_id IS 'Внешний ключ на регион (Dim_Region)';
COMMENT ON COLUMN fact_outcome_forecast.income_outcome_id IS 'Внешний ключ на тип (Dim_IncomeOutcome)';
COMMENT ON COLUMN fact_outcome_forecast.service_id IS 'Внешний ключ на услугу (Dim_OperationType)';
COMMENT ON COLUMN fact_outcome_forecast.amount IS 'Прогнозная сумма';

-- =====================================================
-- 4. ИНДЕКСЫ ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ
-- =====================================================

-- Индексы для fact_intcome_forecast
CREATE INDEX idx_income_forecast_date ON fact_income_forecast(forecast_date_id);
CREATE INDEX idx_income_forecast_subdivision ON fact_income_forecast(subdivision_id);
CREATE INDEX idx_income_forecast_region ON fact_income_forecast(region_id);
CREATE INDEX idx_income_forecast_service ON fact_income_forecast(service_id);

-- Индексы для fact_outcome_forecast
CREATE INDEX idx_outcome_forecast_date ON fact_outcome_forecast(forecast_date_id);
CREATE INDEX idx_outcome_forecast_subdivision ON fact_outcome_forecast(subdivision_id);
CREATE INDEX idx_outcome_forecast_region ON fact_outcome_forecast(region_id);
CREATE INDEX idx_outcome_forecast_service ON fact_outcome_forecast(service_id);