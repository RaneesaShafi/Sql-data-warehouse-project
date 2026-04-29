/*
=====================================================================
Quality Checks
=====================================================================
Script Purpose:
   This script performs quality checks to validate the integrity, consistency and accuracy of the Gold Layer.These checks ensure:
  -Uniqueness of surrogate keys in dimension tables.
  -Referential integrity between fact and dimension tables.
  -Validation of relationships in the date model for analytical purposes.

Usage Notes:
  -Run these checks after data loading silver layer.
  -Investigate and resolve any discrepencies found durig the checks.
=====================================================================

*/

=====================================================================
---.........Data Checking.......----
=====================================================================

---checking duplicates
SELECT cst_id, COUNT(*) from

(Select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gender,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
From silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key =la.cid)t 
GROUP BY cst_id
HAVING COUNT(*)> 1

---Checking integration issue from joining tables
Select 
ci.cst_gender,
ca.gen,
CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender
     ELSE COALESCE(ca.gen, 'n/a')
END AS new_gen
From silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key =la.cid
ORDER BY 1,2

--checking data quality for View--
Select DISTINCT gender From gold.dim_customers;
select * from gold.dim_customers;

--==========Checking for Produsct dim table==========
--checking duplicate/common values in joined tables-----

SELECT prd_key, COUNT(*) From (
Select
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL---Filtering out all old/historical data
)t GROUP BY prd_key
HAVING COUNT(*) >1

select * from gold.dim_products;

---===Checking for Fact table Sales===-----------

---Foreign key Integrity (Dimensions)
--Checking if all dimension tables can successfully join to thr fact table

select * from gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products  p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL

