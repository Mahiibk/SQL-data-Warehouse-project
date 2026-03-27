use DataWarehouse
PRINT '>> Truncating table : silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> INSERTING CLEANED DATA INTO silver.silver.crm_cust_info'
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
)t WHERE flag_last = 1 --removes duplicates from the data


DELETE FROM silver.crm_cust_info
WHERE cst_id IS NULL

SELECT * FROM silver.crm_cust_info
