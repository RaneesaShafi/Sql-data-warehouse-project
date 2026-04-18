/*
=============================================================
DDL Script : Create Bronze Tools
=============================================================
Script Purpose:
      This script creates tables in the 'bronze' schema, dropping existing tables
      if they already exist.
      Run this script to re-define the DDL structure of 'bronze' Tables
=============================================================
*/

--Creating DDL layer---

IF OBJECT_ID ('bronze.crm_cust_info') IS NOT NULL
  DROP TABLE bronze.crm_cust_info;
GO
  
Create table bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gender NVARCHAR(50),
cst_create_date DATE
);
GO

IF OBJECT_ID ('bronze.crm_prd_info ') IS NOT NULL
  DROP TABLE bronze.crm_prd_info ;
GO

Create table bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR (50),
prd_nm NVARCHAR (50),
prd_cost INT,
prd_line NVARCHAR (50),
prd_start_dt DATETIME ,
prd_end_dt DATETIME 
);
GO

IF OBJECT_ID ('bronze.crm_sales_details ') IS NOT NULL
  DROP TABLE bronze.crm_sales_details ;
GO

Create table bronze.crm_sales_details (
sls_ord_num NVARCHAR (50),
sls_prd_key NVARCHAR (50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_pricwe INT
);
GO

IF OBJECT_ID ('bronze.erp_cust_az12 ') IS NOT NULL
  DROP TABLE bronze.erp_cust_az12 ;
GO

Create Table bronze.erp_cust_az12 (
cid NVARCHAR (50),
bdate DATE,
gen NVARCHAR (50)
);
GO

IF OBJECT_ID (' bronze.erp_loc_a101') IS NOT NULL
  DROP TABLE  bronze.erp_loc_a101 ;
GO

Create Table bronze.erp_loc_a101 (
cid NVARCHAR (50),
cntry NVARCHAR (50), 
);
GO

IF OBJECT_ID ('bronze.erp_px_cat_g1v2 ') IS NOT NULL
  DROP TABLE bronze.erp_px_cat_g1v2  ;
GO

Create Table bronze.erp_px_cat_g1v2 (
id NVARCHAR (50),
cat NVARCHAR (50),
subcat NVARCHAR (50),
maintenance NVARCHAR (50)
);
GO
