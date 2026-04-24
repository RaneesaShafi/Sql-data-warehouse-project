/*
=========================================================
Quality Checks
=========================================================
Script Purpose:
This script performs various quality checks for data consistency, accuracy 
and standardization across the 'Silver' schemas.It includes checks for:
-Null or duplicate primary keys.
-Unwanted spaces in string fields.
-Data standardization and consistency.
-Invalid date ranges and orders.
-Dara consistency between related fields.

Uasge Notes:
-Run these checks after data loading Silver Layer.
-Investigate and resolve any discrepancies found during the checks.
=========================================================

*/


---===========DATA CHECKING =========================---
 --check for NULLs or Duplicates in Primary Key
--Expectation: No Result
Select cst_id,
COUNT (*) as total_nulls
From silver.crm_cust_info
GROUP BY cst_id
HAVING  COUNT (*) > 1 OR cst_id IS NULL;


--check for unwanted spaces
--Expectation: No results
select cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

--Data Standardization & Consistency
select DISTINCT  cst_gender 
from silver.crm_cust_info;

select * from silver.crm_cust_info;

-----------------------------------------------

--check for NULLs or Duplicates in Primary Key
--Expectation: No Result

Select cst_id,
COUNT (*) as total_nulls
From bronze.crm_cust_info
GROUP BY cst_id
HAVING  COUNT (*) > 1 OR cst_id IS NULL; --few duplicate values and NULLs found in Primary key CST_ID--

select * 
from bronze.crm_cust_info
where cst_id = 29466;

--check for unwanted spaces
--Expectation: No results
select cst_key
from bronze.crm_cust_info
where cst_key != TRIM(cst_key);

--Data Standardization & Consistency
select DISTINCT  cst_gender 
from bronze.crm_cust_info;


select * 
from (
select *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank_flag
From bronze.crm_cust_info)t
WHERE rank_flag != 1;

---------------cst_PRODUCT_Table---------------------------

--check for NULLs or Duplicates in Primary Key
--Expectation: No Result

Select prd_id,
COUNT (*) as total_nulls
From silver.crm_prd_info
GROUP BY prd_id
HAVING  COUNT (*) > 1 OR prd_id IS NULL;

select * from silver.crm_prd_info

--checking empty spaces 
select prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

--check for NULLs and Negative values
--Expectations:no results
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 OR prd_cost IS NULL;

--Data Standardization & Consistency
select DISTINCT  prd_line 
from silver.crm_prd_info;

--Check for INAVLID Date Orders (end date must not be earlier than start date)
select * 
from silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


----------cst_Sales_Details--------------------------

--Check for Invalid Dates
Select 
NULLIF (sls_order_dt, 0) sls_order_dt
From bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101 
OR sls_order_dt < 19900101 ;

Select 
NULLIF (sls_ship_dt, 0) sls_ship_dt
From bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19900101 ;

Select 
NULLIF (sls_due_dt, 0) sls_due_dt
From bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101 
OR sls_due_dt < 19900101 ;

---check if order date is  HIGHER than ship date OR  due date
select *
from bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

---Check Data Consistency btw : Sales,Quantity and Price
--Sales = Quantity * Price
--Values must not be NULL,zero or negative

select DISTINCT 
sls_sales AS old_sls_sales,
sls_quantity,
sls_pricwe AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS (sls_pricwe)
         THEN sls_quantity * ABS (sls_pricwe)
     ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_pricwe IS NULL OR sls_pricwe <= 0 
       THEN sls_sales / NULLIF(sls_quantity, 0)
      ELSE sls_quantity
END AS   sls_pricwe

From bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_pricwe
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_pricwe IS NULL 
OR sls_sales <= 0  OR sls_quantity <= 0 OR sls_pricwe <= 0
ORDER BY sls_sales, sls_quantity, sls_pricwe;

select DISTINCT 
sls_sales ,
sls_quantity,
sls_pricwe 
From silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_pricwe
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_pricwe IS NULL 
OR sls_sales <= 0  OR sls_quantity <= 0 OR sls_pricwe <= 0
ORDER BY sls_sales, sls_quantity, sls_pricwe

select * from silver.crm_sales_details

----------erp_cust_az12--------------------------
Select 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
bdate,
gen
From silver.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END NOT IN (Select DISTINCT cst_key from silver.crm_cust_info)
;

--Identify out-of-range

select DISTINCT
bdate 
from silver.erp_cust_az12
where bdate < '1924-02-01' OR bdate > GETDATE();

--Dar Standardization & Consistency
select DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F' , 'Female') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M' , 'Male') THEN 'Male'
     ELSE 'n/a'
END gen
from silver.erp_cust_az12

select * from silver.erp_cust_az12

----------erp_loc_a101--------------------------
--Dar Standardization & Consistency

select DISTINCT
cntry
From silver.erp_loc_a101;

select * from silver.erp_loc_a101

----------erp_px_cat_g1v2--------------------------
--check for unwanted values

Select * from bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) 

--Dar Standardization & Consistency
select DISTINCT
maintenance
From bronze.erp_px_cat_g1v2;

--=======================================================================================================
---======Data CLEANING of BRONZE Layer=========================---

---------------cst_CUSTOMER_TABLE---------------------------

--check for NULLs or Duplicates in Primary Key
--Expectation: No Result

Select cst_id,
COUNT (*) as total_nulls
From bronze.crm_cust_info
GROUP BY cst_id
HAVING  COUNT (*) > 1 OR cst_id IS NULL; --few duplicate values and NULLs found in Primary key CST_ID--

select * 
from bronze.crm_cust_info
where cst_id = 29466;

--check for unwanted spaces
--Expectation: No results
select cst_key
from bronze.crm_cust_info
where cst_key != TRIM(cst_key);

--Data Standardization & Consistency
select DISTINCT  cst_gender 
from bronze.crm_cust_info;


select * 
from (
select *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank_flag
From bronze.crm_cust_info)t
WHERE rank_flag != 1;

---------------cst_PRODUCT_Table---------------------------

--check for NULLs or Duplicates in Primary Key
--Expectation: No Result

Select prd_id,
COUNT (*) as total_nulls
From bronze.crm_prd_info
GROUP BY prd_id
HAVING  COUNT (*) > 1 OR prd_id IS NULL;

--checking empty spaces 
select prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm);

--check for NULLs and Negative values
--Expectations:no results
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 OR prd_cost IS NULL;

--Data Standardization & Consistency
select DISTINCT  prd_line 
from bronze.crm_prd_info;

--Check for INAVLID Date Orders (end date must not be earlier than start date)
select * 
from bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

/* select prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test
from bronze.crm_prd_info
where prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');*/

---------------cst_SALES_DETAILS_Table---------------------------
select 
sls_order_dt
from bronze.crm_sales_details
WHERE sls_order_dt < 0;
