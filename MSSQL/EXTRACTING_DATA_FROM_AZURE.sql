use warehouse COMPUTE_WH;
USE ROLE ACCOUNTADMIN;
use database PRICE_ANALYSIS;
use schema foods_and_veg;
USE ROLE ACCOUNTADMIN;

create or replace stage my_blob_stage
URL ='azure://pankajdatalake.blob.core.windows.net/stores'
CREDENTIALS=(AZURE_SAS_TOKEN='sp=racwdlmeop&st=2026-01-06T20:28:38Z&se=2027-01-01T04:43:38Z&spr=https&sv=2024-11-04&sr=c&sig=rSidea4THlktqhafdzM5uullx9DuMc8kugR3NTUjwus%3D')
FILE_FORMAT=(TYPE=PARQUET);

CREATE OR REPLACE TABLE FRESHCO_RAW(
raw VARIANT
);

CREATE OR REPLACE TABLE NoFrills_RAW(
raw VARIANT
);

copy INTO FRESHCO_RAW
from @my_blob_stage/freshco_raw.parquet
FILE_FORMAT = (TYPE = PARQUET);

copy INTO NoFrills_RAW
from @my_blob_stage/no_frills.parquet
FILE_FORMAT = (TYPE = PARQUET);

SELECT
  PARSE_JSON(raw:payload) AS payload_json
FROM NoFrills_RAW
LIMIT 1;

select count(raw) from NoFrills_RAW;

select parse_json(raw:payload) from NoFrills_RAW
limit 1;


create or replace table NoFrills_clean as 
select
payload:articleNumber::bigint as articlenumber,
payload:brand::string as brand,
payload:title::string as name,
payload:pricing:price::float as price,
payload:pricingUnits:type::string as pricingtype,
payload:pricingUnits:unit::string as pricingunit from 
(select PARSE_JSON(raw:payload) AS payload
from NoFrills_RAW
);

select parse_json(raw:payload) from FRESHCO_RAW
limit 1;


create or replace Table FRESHCO_CLEAN AS
SELECT
payload:articleNumber::BIGINT AS articlenumber,
payload:brand::string AS brand,
payload:name::string AS name,
payload:price::float AS price,
payload:itemAmountValue::string AS amountvalue,
payload:uom::string AS pricingtype,
payload:itemAmountUnit::string AS pricingunit

From(select PARSE_JSON(raw:payload) AS payload
from FRESHCO_RAW);