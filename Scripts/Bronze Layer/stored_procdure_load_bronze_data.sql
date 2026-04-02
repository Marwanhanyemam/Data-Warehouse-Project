/*
=======================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=======================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.
*/
create procedure proc_load_bronze as
begin

declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime


set @batch_start_time=getdate();
print'===================='
print'Loading Bronze Layer'
print'===================='

print'------------------'
print'Loading CRM Tables'
print'------------------'

--Table 1:bronze.crm_cust_info
set @start_time = GETDATE()
print'Truncating table : bronze.crm_cust_info'
truncate table bronze.crm_cust_info;

print'Loading Data Into : bronze.crm_cust_info'
bulk insert bronze.crm_cust_info
from 'E:\Marwan\Data with Braa (Youtube)\SQL data warehouse project - Marwan\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
		firstrow = 2,
		fieldterminator = ','
	);
set @end_time = GETDATE()
print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
print'------------------------------------------------------------------------------------------'


--Table 2:bronze.crm_prd_info
set @start_time = GETDATE()
print'Truncating table : bronze.crm_prd_info'
truncate table bronze.crm_prd_info;

print'Loading Data Into : bronze.crm_prd_info'
bulk insert bronze.crm_prd_info
from 'E:\Marwan\Data with Braa (Youtube)\SQL data warehouse project - Marwan\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (firstrow = 2,
	fieldterminator = ','
	);
set @end_time = GETDATE()
print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
print'------------------------------------------------------------------------------------------'

--Table 3:bronze.crm_sales_details
set @start_time = GETDATE()
print'Truncating table : bronze.crm_sales_details'
truncate table bronze.crm_sales_details;

print'Loading Data Into : bronze.crm_sales_details'
bulk insert bronze.crm_sales_details
from 'E:\Marwan\Data with Braa (Youtube)\SQL data warehouse project - Marwan\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (firstrow = 2,
	fieldterminator = ','
	);
set @end_time = GETDATE()
print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
print'------------------------------------------------------------------------------------------'

print'------------------'
print'Loading ERP Tables'
print'------------------'


--Table 4:bronze.erp_cust_az12
set @start_time = GETDATE()
print'Truncating table : bronze.erp_cust_az12'
truncate table bronze.erp_cust_az12;

print'Loading Data Into : bronze.erp_cust_az12'
bulk insert bronze.erp_cust_az12
from 'E:\Marwan\Data with Braa (Youtube)\SQL data warehouse project - Marwan\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
with (firstrow = 2,
	fieldterminator =','
	);
set @end_time = GETDATE()
print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
print'------------------------------------------------------------------------------------------'

--Table 5:bronze.erp_loc_a101
set @start_time = GETDATE()
print'Truncating table : bronze.erp_loc_a101'
truncate table bronze.erp_loc_a101;

print'Loading Data Into : bronze.erp_loc_a101'
bulk insert bronze.erp_loc_a101
from 'E:\Marwan\Data with Braa (Youtube)\SQL data warehouse project - Marwan\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
with (firstrow = 2,
	fieldterminator = ',');
set @end_time = GETDATE()
print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
print'------------------------------------------------------------------------------------------'

--Table 6:bronze.erp_px_cat_g1v2
set @start_time = GETDATE()
print'Truncating table : bronze.erp_px_cat_g1v2'
truncate table bronze.erp_px_cat_g1v2;

print'Loading Data Into : bronze.erp_px_cat_g1v2'
bulk insert bronze.erp_px_cat_g1v2
from 'E:\Marwan\Data with Braa (Youtube)\SQL data warehouse project - Marwan\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
with (firstrow = 2,
	fieldterminator = ',');
set @end_time = GETDATE()
print'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + ' seconds'
print'------------------------------------------------------------------------------------------'

print'=================================='
print'Loading Bronze Layer is Completed'
set @batch_end_time = GETDATE()
print'Total Load Duration: ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar(50)) + ' seconds'
print'=================================='
end

