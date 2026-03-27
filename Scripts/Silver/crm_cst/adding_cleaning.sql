use DataWarehouse
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
use DataWarehouse
--cHECK FOR NULLS OR DUPLICATE IN primary key
--EXCEPTION : NO RESULT

SELECT
cst_id,
count(*)
FROM bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id IS NUll

-- to get duplicate value of cst_id
SELECT
*
FROM(
	select
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
	from bronze.crm_cust_info
)t WHERE flag_last = 1 AND cst_id = 29473

--check the unwanted spaces in string values
--expectation  : No result
SELECT cst_lastname,
cst_firstname
FROM bronze.crm_cust_info
--where cst_lastname != TRIM(cst_lastname) --has a unwanted spaces
where cst_firstname != TRIM(cst_firstname) --has  unwanted space

-- check gender
SELECT cst_gndr
FROM bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr)--no unwated spaces

-- remove the unwanted space
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname)as cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
FROM(
	select
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
	from bronze.crm_cust_info
)t WHERE flag_last = 1 


--Data standardisation & consistancy
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

--converting gendre m = male, f = female
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname)as cst_lastname,
cst_material_status,
CASE WHEN upper(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN upper(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END
cst_create_date
FROM(
	select
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
	from bronze.crm_cust_info
)t WHERE flag_last = 1 

--marrital status
SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info

--changing this into s = single, m = marrital
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname)as cst_lastname,
CASE WHEN upper(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN upper(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,
CASE WHEN upper(TRIM(cst_material_status)) = 's' THEN 'Single'
	WHEN upper(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_material_status, 
cst_create_date
FROM(
	select
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
	from bronze.crm_cust_info
)t WHERE flag_last = 1 

--delete null as primary key
DELETE FROM silver.crm_cust_info
where cst_id IS NULL

--inserting the above query data into silver.crm_cust_info
INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_gndr,
	cst_material_status,
	cst_create_date)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname)as cst_lastname,
CASE WHEN upper(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN upper(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,
CASE WHEN upper(TRIM(cst_material_status)) = 's' THEN 'Single'
	WHEN upper(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_marital_status, 
cst_create_date
FROM(
	select
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
	from bronze.crm_cust_info
)t WHERE flag_last = 1 
DELETE FROM silver.crm_cust_info
where cst_id IS NULL


--checking the data
--nulls or duplicates in silver
SELECT
cst_id,
count(*)
FROM silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id IS NUll

--check the unwanted spaces in string values
--expectation  : No result
SELECT cst_firstname,
cst_lastname
FROM silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname) 

--Data standardisation & consistancy
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

DELETE FROM silver.crm_cust_info
where cst_id IS NULL

select count(*) from silver.crm_cust_info
