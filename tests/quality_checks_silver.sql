/*
=============================================================================================
Quality Checks
=============================================================================================
Script Purpose:
          This script performs various quality checks for data consistency, accuracy 
          and standardization across the 'silver' schemas. I t includes check for :

          - Null or duplicate primary keys.
          - Unwanted spaces in string fields.
          - Data stadardization and consistency.
          - Invalid date ranges and orders.
          - Data consistency between related fields.


Usage Notes :
      - Run these checks after data loading silver layer.
      - Investigate and resolve any discrepancies found during the checks .
==============================================================================================
*/

-- ==============================================================================
-- Checking silver.crm_cust_info;
-- ==============================================================================

SELECT *
FROM bronze.crm_cust_info;

-- Check for nulls or duplicates in primary key
-- Expectations : No Result

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) >1 OR cst_id IS NULL;

SELECT *,
ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id = '29466';

SELECT *,
ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info;


SELECT *
FROM
(SELECT *,
ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info) t
WHERE flag_last >1;


SELECT *
FROM
(SELECT *,
ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) t
WHERE flag_last = 1;


SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key IS NULL;



-- Check for unwanted spaces

SELECT cst_firstname
FROM bronze.crm_cust_info;

SELECT TRIM(cst_firstname)
FROM bronze.crm_cust_info;

SELECT cst_lastname
FROM bronze.crm_cust_info;

SELECT TRIM(cst_lastname)
FROM bronze.crm_cust_info;


-- EXPECTATIONS

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- Data standardization and consistency

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info;

-- ==============================================================================
-- Checking silver.crm_prd_info;
-- ==============================================================================


SELECT *
FROM bronze.crm_prd_info;

SELECT *
FROM silver.crm_prd_info;


-- CHECK FOR NULLS
-- EXPECTATIONS : NO RESULT
SELECT prd_id,
COUNT (*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT (*) > 1 OR prd_id IS NULL;

----------------------------
-- EXPECTATIONS : NO RESULT
SELECT prd_id,
COUNT (*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT (*) > 1 OR prd_id IS NULL;

-- CHECK FOR NULLS

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- CHECK FOR NULLS or NEGATIVE COST

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0  OR prd_cost IS NULL ;


-- DATA STANDARIZATION AND CONSISTENCY 

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


-- CHECK FOR INVALID DATE ORDERS 

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ==============================================================================
-- Checking silver.crm_sales_details;
-- ==============================================================================

SELECT *
FROM silver.crm_sales_details;


WHERE sls_cust_id  not IN (SELECT cst_id FROM silver.crm_cust_info);


-- CHECK FOR INVALID DATES

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT 
NULLIF(sls_ship_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt<= 0
OR LEN(sls_ship_dt)!=8 
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101;

-- CHECK FOR INVALID DATES 
SELECT *
FROM  bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


-- CHECK DATA CONSISTENCY - BETWEEN SALES, QUANTITY AND PRICE
-- >> SALES = QUANTITY * PRICE
-- >> VALUES MUST NOT BE NULL,ZERO, OR NEGATIVE .


SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales  IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
THEN sls_quantity * ABS(sls_price)
ELSE sls_sales END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0
THEN sls_sales/ NULLIF(sls_quantity,0)
ELSE sls_price END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_sales <= 0
OR sls_quantity IS NULL
OR sls_quantity <= 0
OR sls_price IS NULL
OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price;


SELECT DISTINCT
sls_sales 
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_sales <= 0
OR sls_quantity IS NULL
OR sls_quantity <= 0
OR sls_price IS NULL
OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price;



-- ==============================================================================
-- Checking silver.erp_cust_az12;
-- ==============================================================================

SELECT *
FROM silver.erp_cust_az12;



SELECT * FROM silver.crm_cust_info;

-- IDENTIFY OUT-OF-RANGE-DATES 

SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data standardization & Consistency 

SELECT DISTINCT gen,
CASE 
WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;




-- ==============================================================================
-- Checking silver.erp_loc_a101;
-- ==============================================================================


SELECT *
FROM silver.erp_loc_a101;


SELECT*
FROM silver.crm_cust_info;

SELECT *
FROM silver.erp_cust_az12;


SELECT DISTINCT cntry,
CASE 
WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;


-- DATA Standardization and consistency 
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;



-- ==============================================================================
-- Checking silver.erp_px_cat_g1v2;
-- ==============================================================================

SELECT *
FROM silver.erp_px_cat_g1v2;


-- CHECK FOR UNWANTED SPACES

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat ! = TRIM(cat) OR subcat ! = TRIM (subcat) OR maintenance ! = TRIM(maintenance);

-- DATA STANDARIZATION AND CONSISTENCY 
SELECT DISTINCT 
cat 
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT 
subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT 
maintenance
FROM bronze.erp_px_cat_g1v2;
