/*
=====================================================================================
STORED PROCEDURE: BRONZ KATMANI YÜKLE (KAYNAK --> BRONZ)
=====================================================================================
SCRIPT'IN AMACI:
	Bu script csv dosyalarındaki verileri bronze schema'ya yükler
	Aşağıdaki fonksiyonları gerçekleştirir:
	- Önce tüm tabloların içeriklerini TRUNCATE işlemiyle siler.
	- Sonra COPY komutuyla csv dosyalarındaki veriyi bronz tablolarına yerleştirir.
	
KULLANIM ÖRNEĞİ:
	CALL bronze.load_bronze()
=====================================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	baslangic_zamani_bronz 	TIMESTAMP;
	bitis_zamani_bronz		TIMESTAMP;
	baslangic_zamani 		TIMESTAMP;
	bitis_zamani	 		TIMESTAMP;
	gecen_zaman		 		INTERVAL;
BEGIN
	baslangic_zamani_bronz = clock_timestamp();
	RAISE NOTICE 'BRONZE LAYER YÜKLENİYOR';
	
	RAISE NOTICE 'TÜM TABLOLAR TRUNCATE EDİLİYOR';
	TRUNCATE TABLE bronze.crm_cust_info, 
	bronze.crm_prd_info, 
	bronze.crm_prd_sales_details, 
	bronze.erp_cust_az12,
	bronze.erp_loc_a101,
	bronze.erp_px_cat_g1v2;

	RAISE NOTICE 'CRM TABLOLARI YÜKLENİYOR';
	RAISE NOTICE '>> TABLO YÜKLENİYOR: bronze.crm_cust_info';
	baslangic_zamani := clock_timestamp();
	COPY bronze.crm_cust_info
	FROM '/Users/burakyalcin/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	bitis_zamani := clock_timestamp();
	gecen_zaman := bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	RAISE NOTICE '-----------------';

	RAISE NOTICE '>> TABLO YÜKLENİYOR: bronze.crm_prd_info';
	baslangic_zamani := clock_timestamp();
	COPY bronze.crm_prd_info
	FROM '/Users/burakyalcin/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	WITH (FORMAT CSV, HEADER true, DELIMITER ',');
	bitis_zamani := clock_timestamp();
	gecen_zaman := bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	RAISE NOTICE '-----------------';

	RAISE NOTICE '>> TABLO YÜKLENİYOR: bronze.crm_prd_sales_details';
	baslangic_zamani := clock_timestamp();
	COPY bronze.crm_prd_sales_details
	FROM '/Users/burakyalcin/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	WITH (FORMAT CSV, HEADER true, DELIMITER ',');
	bitis_zamani := clock_timestamp();
	gecen_zaman := bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	RAISE NOTICE '-----------------';

	RAISE NOTICE 'ERP TABLOLARI YÜKLENİYOR';

	RAISE NOTICE '>> TABLO YÜKLENİYOR: bronze.erp_cust_az12';
	baslangic_zamani := clock_timestamp();
	COPY bronze.erp_cust_az12
	FROM '/Users/burakyalcin/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
	WITH (FORMAT CSV, HEADER true, DELIMITER ',');
	bitis_zamani := clock_timestamp();
	gecen_zaman := bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	RAISE NOTICE '-----------------';

	RAISE NOTICE '>> TABLO YÜKLENİYOR: bronze.erp_loc_a101';
	baslangic_zamani := clock_timestamp();
	COPY bronze.erp_loc_a101
	FROM '/Users/burakyalcin/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
	WITH (FORMAT CSV, HEADER true, DELIMITER ',');
	bitis_zamani := clock_timestamp();
	gecen_zaman := bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	RAISE NOTICE '-----------------';

	RAISE NOTICE '>> TABLO YÜKLENİYOR: bronze.erp_px_cat_g1v2';
	baslangic_zamani := clock_timestamp();
	COPY bronze.erp_px_cat_g1v2
	FROM '/Users/burakyalcin/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
	WITH (FORMAT CSV, HEADER true, DELIMITER ',');
	bitis_zamani := clock_timestamp();
	gecen_zaman := bitis_zamani - baslangic_zamani;
	RAISE NOTICE 'YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	bitis_zamani_bronz = clock_timestamp();
	gecen_zaman = bitis_zamani_bronz - baslangic_zamani_bronz;
	RAISE NOTICE 'BRONZE LAYER İÇİN YÜKLEME SÜRESİ: % SANİYE', EXTRACT(EPOCH FROM gecen_zaman);
	
EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'HATA: %', SQLERRM;
END;
$BODY$;
ALTER PROCEDURE bronze.load_bronze()
    OWNER TO postgres;
