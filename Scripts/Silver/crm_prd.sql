use DataWarehouse

SELECT prd_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
  FROM bronze.crm_prd_info

--check nulls in primary key
select
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
HAVING count(*) > 1 OR prd_id IS NULL --not any duplicate primary key

--Splitting the prd_key 
SELECT prd_id,
      prd_key,
      REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_')  AS cat_id,-- id in theerp_px_cat_g1v2 has '_' to match id
      SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_')  NOT IN
(SELECT DISTINCT id from bronze.erp_px_cat_g1v2)


SELECT DISTINCT id from bronze.erp_px_cat_g1v2
--prd_key matching with sales table
SELECT prd_id,
      prd_key,
      REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_')  AS cat_id,-- id in theerp_px_cat_g1v2 has '_' to match id
      SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') IN
(SELECT sls_prd_key FROM bronze.crm_sales_details)

--check Spaces unwanted Numbers
--exception : No result
SELECT
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) --not any spaces

--check nulls or negative Numbers
--exception : No result
SELECT
prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL --has nulls

--handlingnulls in prd_cost

SELECT prd_id,
      prd_key,
      REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_')  AS cat_id,-- id in theerp_px_cat_g1v2 has '_' to match id
      SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
      prd_nm,
      ISNULL(prd_cost, 0) as prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') IN
(SELECT sls_prd_key FROM bronze.crm_sales_details)


--Data standardisation and consistancy
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

---pedline value='M' as 'mountain', 'R' as 'Road', s = 'Sales', T as 'Touring'
SELECT prd_id,
      prd_key,
      REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_')  AS cat_id,-- id in theerp_px_cat_g1v2 has '_' to match id
      SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
      prd_nm,
      ISNULL(prd_cost, 0) as prd_cost,
      CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
       END AS prd_line, 
      prd_start_dt,
      prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') 


--Check for invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
where prd_end_dt < prd_start_dt --some data has end date  is greater than start date

--correcting the start and end dates
SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
from bronze.crm_prd_info
where prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509' )  --only for these produt key

--updating the dates into the whole table
SELECT prd_id,
      prd_key,
      REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_')  AS cat_id,-- id in theerp_px_cat_g1v2 has '_' to match id
      SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
      prd_nm,
      ISNULL(prd_cost, 0) as prd_cost,
      CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
       END AS prd_line, 
      CAST(prd_start_dt AS DATE) AS prd_start_dt,
      CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

--Data quality check from silver table, after inserting above modified data query
--check nulls in primary key
select
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
HAVING count(*) > 1 OR prd_id IS NULL

--check Spaces unwanted Numbers
--exception : No result
SELECT
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--check nulls or negative Numbers
--exception : No result
SELECT
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Data standardisation and consistancy
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--Check for invalid Date Orders
SELECT *
FROM silver.crm_prd_info
where prd_end_dt < prd_start_dt

--all data
SELECT * FROM silver.crm_prd_info


  use DataWarehouse
--updating the table
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id	INT,
	cat_id	NVARCHAR(50),
	prd_key	NVARCHAR(50),
	prd_nm	NVARCHAR(50),
	prd_cost	INT,
	prd_line	NVARCHAR(50),
	prd_start_dt	DATE,
	prd_end_dt	DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


--inserting into silver table

PRINT '>> Truncating table : silver.crm_prd_info'
TRUNCATE TABLE silver.crm_sales_details
PRINT '>> INSERTING CLEANED DATA INTO silver.crm_prd_info'
INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_')  AS cat_id,-- id in the erp_px_cat_g1v2 has '_' to match id
    SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) as prd_cost,
    CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
         WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
         ELSE 'n/a'
      END AS prd_line, 
      CAST(prd_start_dt AS DATE) AS prd_start_dt,
      CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

select * from silver.crm_prd_info
