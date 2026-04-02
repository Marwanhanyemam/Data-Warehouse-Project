/*
===================================
Stored Procedure: Load Silver Layer
===================================
Script Purpose: This stored procedure performs the ETL (extract, transorm, load) process on the 'bronze' schema to load it into 'silver' schema
	Actions performed:
		-Truncates silver tables
		-Inserts and loads transformed and cleansed data from bronze layer into silver layer
*/
create procedure proc_load_silver as
Begin

	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime

	set @batch_start_time=getdate()
	print'===================='
	print'Loading Silver Layer'
	print'===================='

	print'------------------'
	print'Loading CRM Tables'
	print'------------------'

	--Table 1: silver.crm_cust_info
	set @start_time = getdate()
	print'Truncating Table: silver.crm_cust_info'
	truncate table silver.crm_cust_info;

	print'Inserting data into: silver.crm_cust_info'
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
		trim(cst_firstname) as first_name,  --Remove unwanted spaces in first_name column
		trim(cst_lastname) as last_name,	--Remove unwanted spaces in last_name column
		Case								--Normalize marital status
			when upper(trim(cst_marital_status)) = 'M' Then 'Married'
			When upper(trim(cst_marital_status)) = 'S' Then 'Single'
			Else 'N/A'
		End as Marital_status,

		Case								--Normalize gender
			when upper(trim(cst_gndr)) = 'M' Then 'Male'
			When upper(trim(cst_gndr)) = 'F' Then 'Female'
			Else 'N/A'
		End as gender,

		cast(cst_create_date as date) as create_date		--change datatype of cst_create_date to time instead of datetime

											--Remove nulls and duplicates in the primary key
	from(
	select *,
	row_number() over(partition by cst_id order by cst_create_date desc) as latest_cst_create_date
	from bronze.crm_cust_info
	where cst_id is not null
	) t where latest_cst_create_date = 1

	set @end_time = getdate()
	print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
	print'------------------'

	--Table 2: silver.crm_prd_info
	set @start_time = getdate()
	print'Truncating Table: silver.crm_prd_info'
	Truncate table silver.crm_prd_info

	print'Inserting data into: silver.crm_prd_info'
	insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_date,
		prd_end_date)

	select 
		prd_id,
		replace(substring(prd_key,1,5),'-','_') as cat_id,     --extract category id
		substring(prd_key,7,len(prd_key)) as prd_key,			--extract product key
		prd_nm,
		isnull(prd_cost,0) as prd_cost,							--replace nulls in prd_cost with 0
		case upper(trim(prd_line))							--Normalize prd_line
		when 'R' Then 'Road'
		when 'S' Then 'Other Sales'
		when 'M' Then 'Mountain'
		when 'T' Then 'Touring'
		else 'N/A'
		End as prd_line,
		cast(prd_start_date as date) as prd_start_date,
		cast(lead(prd_start_date) over(partition by prd_key order by prd_start_date)-1 as date) as prd_end_date --calculate the end date as one day before the next start date
	
	from bronze.crm_prd_info

	SET @end_time = getdate()
	print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
	print'------------------'


	--Table 3: silver.crm_sales_details
	set @start_time = getdate()
	print'Truncating Table: silver.crm_sales_details'
	truncate table silver.crm_sales_details

	print'Inserting data into: silver.crm_sales_details'
	insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)


	select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case		--cleans invalid sls_order_dt values
			when sls_order_dt = 0 or len(sls_order_dt) != 8 Then NULL
			else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case		--cleans invalid sls_ship_dt values
			when sls_ship_dt = 0 or len(sls_ship_dt) != 8 Then NULL
			else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case		--cleans invalid sls_due_dt values
			when sls_due_dt = 0 or len(sls_due_dt) != 8 Then NULL
			else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		case		--cleans invalid sls_sales values
			when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) Then sls_quantity * abs(sls_price) 
			else sls_sales
			end as sls_sales,
		sls_quantity,
		case		--cleans invalid sls_price values
			when sls_price is null or sls_price <=0 Then sls_sales / nullif(sls_quantity,0 )
			when sls_price != sls_sales / sls_quantity Then sls_sales / sls_quantity
			else sls_price
			end as sls_price

	from bronze.crm_sales_details

	SET @end_time = getdate()
	print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
	print'------------------'

	print'------------------'
	print'Loading ERP Tables'
	print'------------------'

	--Table 4: silver.erp_cust_az12
	set @start_time = getdate()
	print'Truncating Table: silver.erp_cust_az12'
	truncate table silver.erp_cust_az12

	print'Inserting data into: silver.erp_cust_az12'
	insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen)

	select 
		case  --remove 'NAS' prefix if exists
			when cid like 'NASA%' Then substring(cid,4,len(cid))
			else cid
		end as cid,
		case	--set future birthdates to null
			when bdate > getdate() Then null
			else bdate
		end as bdate,
		case  --normalize gender
		when upper(trim(gen)) like 'M%' Then 'Male'
		when upper(trim(gen)) like 'F%' Then 'Female'
		else 'N/A'
		end as gen
	from bronze.erp_cust_az12

	set @end_time = getdate()
	print'Load Duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
	print'------------------'

	--Table 5: silver.erp_loc_a101
	set @start_time = getdate()
	print'Truncating Table: silver.erp_loc_a101'
	truncate table silver.erp_loc_a101

	print'Inserting data into: silver.erp_loc_a101'
	insert into silver.erp_loc_a101(
		cid,
		cntry)

	select 
		replace(cid,'-','') as cid,
		case	--normalize country names and handling missing/blank cells
		when trim(cntry) = 'DE' Then 'Germany'
		when trim(cntry) in('US','USA') Then 'United States'
		when trim(cntry) = '' or cntry is null Then 'N/A'
		else trim(cntry)
		end as cntry
	from bronze.erp_loc_a101

	SET @end_time = getdate()
	print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
	print'------------------'

	--Table 6: silver.erp_px_cat_g1v2
	set @start_time = getdate()
	print'Truncating Table: silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2

	print'Inserting data into: silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)

	select 
		id,
		cat,
		subcat,
		maintenance
	from bronze.erp_px_cat_g1v2

	set @end_time = getdate()
	print'Load Duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
	print'------------------'

	set @batch_end_time = getdate()
	print'==========================================================='
	print'Total Load Duration ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar(50)) + ' seconds'
	print'==========================================================='

End
