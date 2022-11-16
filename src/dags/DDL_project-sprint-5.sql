------Задумка в том, чтобы логика обновлений была в хранимых процедурах с неймингами таблиц, а Airflow запускал их втребуемой очерёдности

------------------------------------------------------------------------------------------------------------------------
--------------------------------------DDL Staging for api---------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-----Сначала данные в неизменённом виде

CREATE SCHEMA if not exists stg;

--stg.api_couriers
drop table if exists stg.api_couriers;
create table if not exists stg.api_couriers(
id serial primary key,
content text unique,
load_ts timestamp default current_timestamp
);


--stg.api_deliveries
drop table if exists stg.api_deliveries;
create table if not exists stg.api_deliveries(
id serial primary key,
content jsonb unique,
load_ts timestamp default current_timestamp
);


------------------------------------------------------------------------------------------------------------------------
--------------------------------------DDL Scripts Staging---------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-----Теперь данные в табличном виде с проверками

-----Таблица для курьеров
drop table if exists stg.deliverysystem_couriers;
CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
courier_id VARCHAR NOT NULL UNIQUE,
courier_name VARCHAR NOT NULL,
load_ts timestamp NOT NULL);

drop PROCEDURE if exists Load_stg_couriers ();
CREATE PROCEDURE Load_stg_couriers()
LANGUAGE SQL
AS $$
TRUNCATE TABLE stg.deliverysystem_couriers;
INSERT INTO stg.deliverysystem_couriers (courier_id, courier_name, load_ts)
SELECT
    content::json#>>'{_id}' as _id
  , content::json#>>'{name}' as name
  , load_ts
FROM stg.api_couriers;
$$;

-----Таблица для доставок
drop table if exists stg.deliverysystem_deliveries;
CREATE TABLE stg.deliverysystem_deliveries (
id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
order_id VARCHAR NOT NULL,
order_ts timestamp NOT NULL,
delivery_id VARCHAR NOT NULL,
courier_id VARCHAR NOT NULL,
address VARCHAR NOT NULL,
delivery_ts timestamp NOT NULL,
sum numeric(14, 5) NOT NULL,
tip_sum numeric(14, 5) NOT NULL DEFAULT 0 CHECK (tip_sum >= 0), rate int NOT NULL DEFAULT 0 CHECK (rate >= 1 and rate <= 5),
RATE int NOT NULL,
load_ts timestamp NOT NULL);

drop PROCEDURE if exists Load_stg_deliveries();
CREATE PROCEDURE Load_stg_deliveries()
LANGUAGE SQL
AS $$
TRUNCATE TABLE stg.deliverysystem_deliveries;
INSERT INTO  stg.deliverysystem_deliveries
(
order_id,
order_ts,
delivery_id,
courier_id,
address,
delivery_ts,
rate,
sum,
tip_sum,
load_ts)
    SELECT
       content::json#>>'{order_id}' order_id
     , CAST(content::json#>>'{order_ts}' as timestamp)::timestamptz order_ts
     , content::json#>>'{courier_id}' courier_id
     , content::json#>>'{delivery_id}' delivery_id
     , content::json#>>'{address}' address
     , CAST(content::json#>>'{delivery_ts}' as timestamp)::timestamptz delivery_ts
     , CAST(content::json#>>'{rate}' AS int) rate
     , CAST(content::json#>>'{sum}' AS numeric(14, 5)) sum
     , CAST(content::json#>>'{tip_sum}'AS numeric(14, 5)) tip_sum
     , load_ts
    FROM stg.api_deliveries;
$$;

------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------DDL Scripts ODS----------------------------------------------------------
----------
--------------------------------------------------------------------------------------------------------------

-----заливка в таблицы с историей хранения

CREATE SCHEMA if not exists dds;

-----Таблица измерений по курьерам
drop table if exists dds.couriers;
CREATE TABLE dds.couriers (
id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
courier_id VARCHAR NOT NULL,
courier_name VARCHAR NOT NULL,
load_ts timestamp NOT NULL,
update_ts timestamp
                             );

drop PROCEDURE if exists Load_dds_couriers();
CREATE PROCEDURE Load_dds_couriers()
LANGUAGE SQL
AS $$
INSERT INTO dds.couriers (courier_id, courier_name, load_ts)
SELECT courier_id, courier_name, load_ts FROM stg.deliverysystem_couriers
WHERE courier_id not in (SELECT courier_id FROM dds.couriers);
UPDATE ods.couriers SET (courier_id, courier_name, update_ts) =
(SELECT courier_id, courier_name, load_ts FROM stg.deliverysystem_couriers
WHERE courier_id in (SELECT courier_id FROM dds.couriers)
AND deliverysystem_couriers.courier_id = couriers.courier_id);
$$;



-----Таблица измерений по заказам

drop table if exists dds.orders CASCADE;
CREATE TABLE dds.orders (
id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
order_id VARCHAR NOT NULL,
order_ts timestamp NOT NULL,
courier_id VARCHAR NOT NULL,
delivery_ts timestamp NOT NULL,
sum numeric(14, 5) NOT NULL,
tip_sum numeric(14, 5) NOT NULL,
rate int,
load_ts timestamp NOT NULL);

-----Ссылка на таблицу измерений по курьерам
ALTER TABLE dds.orders
ADD COLUMN couriered int NULL REFERENCES dds.couriers(id);

-----Обновление

drop PROCEDURE if exists Load_dds_orders();
CREATE PROCEDURE Load_dds_orders()
LANGUAGE SQL
AS $$
INSERT INTO dds.orders (order_id, order_ts, courier_id, delivery_ts, sum, tip_sum, rate, load_ts)
SELECT order_id, order_ts, courier_id, delivery_ts, sum, tip_sum, rate, load_ts FROM stg.deliverysystem_deliveries
WHERE order_id not in (SELECT order_id FROM dds.orders);

$$;

------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------DDL Scripts DM----------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

----Финальная витрина данных
drop table if exists cdm.dm_courier_ledger;
CREATE TABLE cdm.dm_courier_ledger (
id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
courier_id VARCHAR NOT NULL,
courier_name VARCHAR NOT NULL,
settlement_year SMALLINT NOT NULL CHECK(settlement_year>= 2022 AND settlement_year < 2500),
settlement_month SMALLINT NOT NULL CHECK(settlement_month>= 1 AND settlement_month <= 12),
orders_count int NOT NULL CHECK(orders_count >= 0),
orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
rate_avg numeric(14, 5) NOT NULL DEFAULT 0 CHECK (rate_avg >= 0),
order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0),
UNIQUE(courier_id, settlement_year, settlement_month));


drop PROCEDURE if exists Load_dm_courier_ledger();
CREATE PROCEDURE Load_dm_courier_ledger()
LANGUAGE SQL

AS $$

DELETE FROM cdm.dm_courier_ledger
WHERE settlement_year = (SELECT DISTINCT extract(YEARS FROM order_ts) FROM stg.deliverysystem_deliveries)
AND settlement_month = (SELECT DISTINCT extract(MONTH FROM order_ts) FROM stg.deliverysystem_deliveries);

INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year,
settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum,
courier_tips_sum, courier_reward_sum)
SELECT
couriers.courier_id,
couriers.courier_name,
extract(YEARS FROM orders.order_ts) y,
extract(MONTH FROM orders.order_ts) m,
COUNT(orders.order_id) orders_count,
SUM(sum) orders_total_sum,
AVG(rate) rate_avg,
SUM(sum) * 0.25 order_processing_fee,
CASE WHEN AVG(rate) < 4  AND SUM(sum) > 100 THEN SUM(sum) * 0.05
WHEN AVG(rate) < 4  AND SUM(sum) <= 100 THEN 100
WHEN AVG(rate) between 4 AND 4.5  AND SUM(sum) > 150 THEN SUM(sum) * 0.07
WHEN AVG(rate) between 4 AND 4.5  AND SUM(sum) <= 150 THEN 150
WHEN AVG(rate) between 4.5 AND 4.9  AND SUM(sum) > 175  THEN SUM(sum) * 0.08
WHEN AVG(rate) between 4.5  AND 4.9 AND SUM(sum) <= 175  THEN 150
WHEN AVG(rate) > 4.9  AND SUM(sum) > 200  THEN SUM(sum) * 0.1
WHEN AVG(rate) > 4.9 AND SUM(sum) <= 200  THEN 200
END courier_order_sum,
SUM(tip_sum) courier_tips_sum,
(
CASE WHEN AVG(rate) < 4  AND SUM(sum) > 100 THEN SUM(sum) * 0.05
WHEN AVG(rate) < 4  AND SUM(sum) <= 100 THEN 100
WHEN AVG(rate) between 4 AND 4.5  AND SUM(sum) > 150 THEN SUM(sum) * 0.07
WHEN AVG(rate) between 4 AND 4.5  AND SUM(sum) <= 150 THEN 150
WHEN AVG(rate) between 4.5 AND 4.9  AND SUM(sum) > 175  THEN SUM(sum) * 0.08
WHEN AVG(rate) between 4.5  AND 4.9 AND SUM(sum) <= 175  THEN 150
WHEN AVG(rate) > 4.9  AND SUM(sum) > 200  THEN SUM(sum) * 0.1
WHEN AVG(rate) > 4.9 AND SUM(sum) <= 200  THEN 200
END + SUM(tip_sum) ) * 0.95 courier_reward_sum
FROM dds.orders AS orders
INNER JOIN dds.couriers AS couriers
ON orders.courier_id = couriers.courier_id
WHERE extract(YEARS FROM orders.order_ts) = (SELECT DISTINCT extract(YEARS FROM order_ts) FROM stg.deliverysystem_deliveries)
AND extract(MONTH FROM orders.order_ts) = (SELECT DISTINCT extract(MONTH FROM order_ts) FROM stg.deliverysystem_deliveries)
GROUP BY
couriers.courier_id,
couriers.courier_name,
extract(YEARS FROM orders.order_ts) ,
extract(MONTH FROM orders.order_ts)

$$;