/*
=======================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=======================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before laoding data.
  - Uses the 'COPY' command to load data from csv files to bronze tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage example:
CALL bronze.load_bronze();
======================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
	DECLARE 
		start_time TIMESTAMP;
		end_time TIMESTAMP;
		batch_start_time TIMESTAMP;
		batch_end_time TIMESTAMP;
	
	BEGIN
		batch_start_time := clock_timestamp();
		RAISE NOTICE '======================';
		RAISE NOTICE 'Loading Bronze Layer';
		RAISE NOTICE '======================';
		
		RAISE NOTICE '---------------------';
		RAISE NOTICE 'Loading CRM tables';
		RAISE NOTICE '---------------------';
		
		BEGIN
			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating table:  bronze.crm_cust_info';
			TRUNCATE TABLE bronze.crm_cust_info;
			
			RAISE NOTICE '>>Inserting Data Into:  bronze.crm_cust_info';
			COPY bronze.crm_cust_info
			from 'C:\tmp\datasets\source_crm\cust_info.csv'
			with (
				FORMAT CSV,
				HEADER true,
				DELIMITER ','
			);
			
			RAISE NOTICE '>>>crm_cust_info loaded successfully';
		EXCEPTION WHEN OTHERS THEN 
				RAISE NOTICE 'Error loading crm_cust_info: %',SQLERRM;
				RAISE;
		END;
		end_time := clock_timestamp();
		RAISE NOTICE '>>CRM cust table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
	
		BEGIN	
			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating table:  bronze.crm_prd_info';
			TRUNCATE TABLE bronze.crm_prd_info;
			
			RAISE NOTICE '>>Inserting Data Into:  bronze.crm_prd_info';
			COPY bronze.crm_prd_info
			from 'C:\tmp\datasets\source_crm\prd_info.csv'
			with (
				FORMAT CSV,
				HEADER true,
				DELIMITER ','
			);
	
			RAISE NOTICE 'crm_prd_info loaded successfully';
	
		EXCEPTION
			WHEN OTHERS THEN
				RAISE NOTICE 'Error loading crm_prd_info: %',SQLERRM;
				RAISE;
		END;
		end_time := clock_timestamp();
		RAISE NOTICE '>>CRM prd table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
		
		BEGIN
			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating table:  bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details;
			
			RAISE NOTICE '>>Inserting Data Into:  bronze.crm_sales_details';
			COPY bronze.crm_sales_details
			from 'C:\tmp\datasets\source_crm\sales_details.csv'
			with (
				FORMAT CSV,
				HEADER true,
				DELIMITER ','
			);
	
			RAISE NOTICE 'crm_sales_details loaded successfully';
	
		EXCEPTION
			WHEN OTHERS THEN
				RAISE NOTICE 'Error loading crm_sales_details: %',SQLERRM;
				RAISE;
		END;
		end_time := clock_timestamp();
		RAISE NOTICE '>>CRM sales table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
		
		RAISE NOTICE '---------------------';
		RAISE NOTICE 'Loading ERP tables';
		RAISE NOTICE '---------------------';
	
		BEGIN
			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating table:  bronze.erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12;
			
			RAISE NOTICE '>>Inserting Data Into:  bronze.erp_cust_az12';
			COPY bronze.erp_cust_az12
			from 'C:\tmp\datasets\source_erp\CUST_AZ12.csv'
			with (
				FORMAT CSV,
				HEADER true,
				DELIMITER ','
			);
	
			RAISE NOTICE 'erp_cust_az12 loaded successfully';
	
		EXCEPTION
	    WHEN OTHERS THEN
	        RAISE NOTICE 'Error loading erp_cust_az12: %', SQLERRM;
			RAISE;
		END;
		end_time := clock_timestamp();
		RAISE NOTICE '>>ERP cust table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
		
		BEGIN
			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating table:  bronze.erp_loc_a101';
			TRUNCATE TABLE bronze.erp_loc_a101;
			
			RAISE NOTICE '>>Inserting Data Into:  bronze.erp_loc_a101';
			COPY bronze.erp_loc_a101
			from 'C:\tmp\datasets\source_erp\LOC_A101.csv'
			with (
				FORMAT CSV,
				HEADER true,
				DELIMITER ','
			);
	
			RAISE NOTICE 'erp_loc_a101 loaded successfully';
		
		EXCEPTION
			WHEN OTHERS THEN
				RAISE NOTICE 'Error loading erp_loc_a101: %',SQLERRM;
				RAISE;
		END;
		end_time := clock_timestamp();
		RAISE NOTICE '>>ERP loc table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
	
		BEGIN
			start_time := clock_timestamp();
			RAISE NOTICE '>> Truncating table:  bronze.erp_px_cat_g1v2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			
			RAISE NOTICE '>>Inserting Data Into:  bronze.erp_px_cat_g1v2';
			COPY bronze.erp_px_cat_g1v2
			from 'C:\tmp\datasets\source_erp\PX_CAT_G1V2.csv'
			with (
				FORMAT CSV,
				HEADER true,
				DELIMITER ','
			);
	
			RAISE NOTICE 'erp_px_cat_g1v2 loaded successfully';
	
		EXCEPTION 
			WHEN OTHERS THEN
				RAISE NOTICE 'Error laoding erp_px_cat_g1v2:% ',SQLERRM;
				RAISE;
		END;
		end_time := clock_timestamp();
		RAISE NOTICE '>>ERP px table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);

		batch_end_time := clock_timestamp();
		RAISE NOTICE '>>Total Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (batch_end_time-batch_start_time))::numeric,2);
		
END;
$$;

SET client_min_messages TO NOTICE;
CALL bronze.load_bronze();
