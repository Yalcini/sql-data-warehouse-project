/*
=====================================================================================
STORED PROCEDURE: SILVER KATMANI YÜKLE (BRONZE --> SILVER)
=====================================================================================
SCRIPT'IN AMACI:
	Bu script bronze schemada bulunan tablolar üzerinde ETL (Extract, Transform, Load)
  işlemlerini yaparak silver schemadaki tabloları doldurur.
	Aşağıdaki fonksiyonları gerçekleştirir:
	- Önce tüm tabloların içeriklerini TRUNCATE işlemiyle siler.
	- Sonra transform edilmiş temizlenmiş dataları silver schemadaki tablolara insert eder.
	
KULLANIM ÖRNEĞİ:
	CALL silver.load_silver();
=====================================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver() LANGUAGE 'plpgsql'
AS $$
DECLARE
	baslangic_zamani_silver TIMESTAMP;
	bitis_zamani_silver		TIMESTAMP;
	baslangic_zamani 		TIMESTAMP;
	bitis_zamani	 		TIMESTAMP;
	gecen_zaman		 		INTERVAL;
BEGIN
	baslangic_zamani_silver = clock_timestamp();
	
	RAISE NOTICE 'TÜM TABLOLAR TRUNCATE EDİLİYOR';
	TRUNCATE TABLE silver.crm_cust_info, silver.crm_prd_info, silver.crm_prd_sales_details,
					silver.erp_cust_az12, silver.erp_loc_a101, silver.erp_px_cat_g1v2;
					
	RAISE NOTICE '>> TABLO YÜKLENİYOR: silver.crm_cust_info';
	
	baslangic_zamani = clock_timestamp();
	
	INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status))= 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM(
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	) WHERE flag_last = 1;
	
	bitis_zamani = clock_timestamp();
	gecen_zaman = bitis_zamani - baslangic_zamani;

	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	
	--#####################################################################################
	RAISE NOTICE '>> TABLO YÜKLENİYOR: silver.crm_prd_info';
	baslangic_zamani = clock_timestamp();
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
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- deriving category id from product key
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,	   -- also deriving product key from within
	prd_nm,
	CASE WHEN prd_cost IS NULL THEN 0
		 ELSE prd_cost
	END AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Toruing'
		 ELSE 'n/a'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt, -- we get rid of timestamp info cause it was all zeros
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info;
	
	bitis_zamani = clock_timestamp();
	gecen_zaman = bitis_zamani - baslangic_zamani;
	
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	
	--#####################################################################################
	RAISE NOTICE '>> TABLO YÜKLENİYOR: silver.crm_prd_sales_details';
	baslangic_zamani = clock_timestamp();
	INSERT INTO silver.crm_prd_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
		 ELSE TO_DATE(CAST(sls_order_dt AS TEXT), 'YYYYMMDD')
	END AS sls_order_dt,
	
	CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
		 ELSE TO_DATE(CAST(sls_ship_dt AS TEXT), 'YYYYMMDD')
	END AS sls_ship_dt,
	
	CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
		 ELSE TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD')
	END AS sls_due_dt,
	
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, --recalculate sales if original data is missing or incorrect
	
	sls_quantity, 
	
	CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price --derive price if original data is invalid
	FROM bronze.crm_prd_sales_details;
	bitis_zamani = clock_timestamp();
	gecen_zaman = bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	
	--#####################################################################################
	RAISE NOTICE '>> TABLO YÜKLENİYOR: silver.erp_cust_az12';
	baslangic_zamani = clock_timestamp();
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	SELECT
	CASE WHEN cid LIKE ('NAS%') 
		THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END AS cid,
	CASE WHEN bdate >= CURRENT_DATE
		THEN NULL
		ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;
	bitis_zamani = clock_timestamp();
	gecen_zaman = bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	
	--#####################################################################################
	RAISE NOTICE '>> TABLO YÜKLENİYOR: silver.erp_loc_a101';
	baslangic_zamani = clock_timestamp();
	INSERT INTO silver.erp_loc_a101(cid, cntry)
	SELECT
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101;
	bitis_zamani = clock_timestamp();
	gecen_zaman = bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	
	--#####################################################################################
	RAISE NOTICE '>> TABLO YÜKLENİYOR: silver.erp_px_cat_g1v2';
	baslangic_zamani = clock_timestamp();
	INSERT INTO silver.erp_px_cat_g1v2
	(id, cat, subcat, maintenance)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	bitis_zamani = clock_timestamp();
	gecen_zaman = bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	
	bitis_zamani_silver = clock_timestamp();
	gecen_zaman = bitis_zamani_silver - baslangic_zamani_silver;
	RAISE NOTICE 'SILVER LAYER İÇİN YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
END;
$$;
