use warehouse COMPUTE_WH;
USE ROLE ACCOUNTADMIN;
USE SCHEMA FOODS_AND_VEG;
USE DATABASE price_analysis;
ALTER DATABASE price_analysis RENAME TO PRICE_ANALYSIS;
USE DATABASE PRICE_ANALYSIS;

SHOW TABLES IN PRICE_ANALYSIS.FOODS_AND_VEG;

SELECT * FROM FOODS_AND_VEG.FRESHCO_CLEAN
limit 1;

SELECT * FROM FOODS_AND_VEG.NoFrills_clean
WHERE name LIKE '%bread%';

SELECT * FROM FOODS_AND_VEG.FRESHCO_CLEAN
WHERE name LIKE '%bread%';

use schema foods_and_veg;

Select f.articlenumber,
f.name,
n.name,
f.price,
n.price,
f.pricingtype,
n.pricingtype 
from FRESHCO_CLEAN f
join NoFrills_clean n on f.articlenumber = n.articlenumber;



