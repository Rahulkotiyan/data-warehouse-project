
/*
==============================================================
Stored Procedure: Load Silver Layer(Bronze -> Silver)
==============================================================
Script Purpose:
  This stored procedure performs the ETL(Extract, Transform, Load) process to 
  populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
  - Truncates silver tables.
  - Inserts transformed and cleansed data from Bronze into silver tables

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  CALL silver.load_silver();
===================================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
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
	RAISE NOTICE 'Loading Silver Layer';
	RAISE NOTICE '======================';
		
	RAISE NOTICE '---------------------';
	RAISE NOTICE 'Loading CRM tables';
	RAISE NOTICE '---------------------';
	
	BEGIN
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating table:  silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;			
		RAISE NOTICE '>>Inserting Data Into:  silver.crm_cust_info';
		insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		case when upper(trim(cst_material_status)) = 'S' then 'Single'
			 when upper(trim(cst_material_status)) = 'M' then 'Married'
			 else 'n/a'
		end cst_material_status,    --Normalize marital status values to readable format
		case when upper(trim(cst_gndr)) = 'F' then 'Female'
			 when upper(trim(cst_gndr)) = 'M' then 'Male'
			 else 'n/a'
		end cst_gndr,  --Normalize gender values to readable format
		cst_create_date
		from(
		select
		*,
		row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null
		)t where flag_last=1;
		RAISE NOTICE '>>>crm_cust_info inserted successfully';
		EXCEPTION WHEN OTHERS THEN 
				RAISE NOTICE 'Error inserting crm_cust_info: %',SQLERRM;
				RAISE;
	END;
	end_time := clock_timestamp();
	RAISE NOTICE '>>CRM cust table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
	
	---------------------------------------------
	BEGIN
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating table:  silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;			
		RAISE NOTICE '>>Inserting Data Into:  silver.crm_prd_info';
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
		select
		prd_id,
		replace(substring(prd_key,1,5),'-','_') as cat_id,
		substring(prd_key,7,length(prd_key)) as prd_key,
		prd_nm,
		COALESCE(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'Other Sales'
			 when 'T' then 'Touring'
			 else 'n/a'
		end as prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt) - INTERVAL '1 day' AS DATE) as prd_end_dt
		from bronze.crm_prd_info;
		RAISE NOTICE 'crm_prd_info inserted successfully';
	
		EXCEPTION
			WHEN OTHERS THEN
				RAISE NOTICE 'Error inserting crm_prd_info: %',SQLERRM;
				RAISE;
	END;
	end_time := clock_timestamp();
	RAISE NOTICE '>>CRM prd table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
	
	-------------------------------------------------
	
	
	BEGIN
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating table:  silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;			
		RAISE NOTICE '>>Inserting Data Into:  silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
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
		select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt :: TEXT)!=8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt :: TEXT)!=8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt :: TEXT)!=8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
				THEN sls_quantity*ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price<=0
				THEN sls_sales/NULLIF(sls_quantity,0)
			 ELSE sls_price
		END AS sls_price
		from bronze.crm_sales_details;
		RAISE NOTICE 'crm_sales_details inserted successfully';
	
		EXCEPTION
			WHEN OTHERS THEN
				RAISE NOTICE 'Error inserting crm_sales_details: %',SQLERRM;
				RAISE;
	END;
	end_time := clock_timestamp();
	RAISE NOTICE '>>CRM sales table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
	
	---------------------------------------------------------------
	RAISE NOTICE '---------------------';
	RAISE NOTICE 'Loading ERP tables';
	RAISE NOTICE '---------------------';
	
	BEGIN
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating table:  silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;			
		RAISE NOTICE '>>Inserting Data Into:  silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		select
		case when cid like 'NAS%' then substring(cid,4,LENGTH(cid))
			 else cid
		end cid,
		case when bdate>NOW() THEN NULL
			 else bdate
		end as bdate,
		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			 when upper(trim(gen)) in ('M','MALE') then 'Male'
			 else 'n/a'
		end as gen
		from bronze.erp_cust_az12;
		RAISE NOTICE 'erp_cust_az12 inserting successfully';
	
		EXCEPTION
	    WHEN OTHERS THEN
	        RAISE NOTICE 'Error inserting erp_cust_az12: %', SQLERRM;
			RAISE;
	END ;
	end_time := clock_timestamp();
	RAISE NOTICE '>>ERP cust table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
	
	-----------------------------------------------------------------
	
	
	BEGIN
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating table:  silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;			
		RAISE NOTICE '>>Inserting Data Into:  silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(cid,cntry)
		select
		REPLACE(cid,'-','') cid,
		case when TRIM(cntry) = 'DE' THEN 'Germany'
			 when TRIM(cntry) IN ('US','USA') THEN 'United States'
			 when TRIM(cntry) ='' OR cntry IS NULL THEN 'n/a'
			 else TRIM(cntry)
		end as cntry
		from bronze.erp_loc_a101;
		RAISE NOTICE 'erp_loc_a101 inserting successfully';
		
		EXCEPTION
			WHEN OTHERS THEN
				RAISE NOTICE 'Error inserting erp_loc_a101: %',SQLERRM;
				RAISE;
	END ;
	end_time := clock_timestamp();
	RAISE NOTICE '>>ERP loc table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);
	
	-------------------------------------------------------------------
	BEGIN
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating table:  silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;			
		RAISE NOTICE '>>Inserting Data Into:  silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		RAISE NOTICE 'erp_px_cat_g1v2 inserting successfully';
	
		EXCEPTION 
			WHEN OTHERS THEN
				RAISE NOTICE 'Error inserting erp_px_cat_g1v2:% ',SQLERRM;
				RAISE;
	END;
	end_time := clock_timestamp();
	RAISE NOTICE '>>ERP px table load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time-start_time))::numeric,2);

	batch_end_time := clock_timestamp();
	RAISE NOTICE '>>Total Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (batch_end_time-batch_start_time))::numeric,2);
	
END;
$$;

SET client_min_messages TO NOTICE;
CALL silver.load_silver();
