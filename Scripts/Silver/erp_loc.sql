USE DataWarehouse

SELECT
cid,
cntry
FROM bronze.erp_loc_a101


--comparing with cust information table common column is cst id
SELECT
cst_key from silver.crm_cust_info

--changing the cid in the form of cst_key
--now this becomes same
SELECT
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101 --now this becomes same

--
SELECT
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)


--Data standardisation and consistancy
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry   ---no realy good

--correcting data
SELECT
cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = ' ' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101

--update the query into newone
SELECT
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = ' ' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101


use DataWarehouse

INSERT INTO silver.erp_loc_a101(
cid,
cntry
)
SELECT
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United Sates'
	WHEN TRIM(cntry) = ' ' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101

SELECT * FROM silver.erp_loc_a101
