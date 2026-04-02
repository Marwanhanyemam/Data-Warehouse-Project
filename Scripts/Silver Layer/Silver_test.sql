/*Test for table 1: bronze.crm_cust_info  */

--Checking if there is any nulls or duplicates in the primary key
--Expectation: No results
select cst_id, count(*) 
from bronze.crm_cust_info
group by cst_id
having count(*) >1 or cst_ID is null


--Checking if there is unwanted spaces
--Expectation: No results
select cst_key ,cst_firstname ,cst_lastname
from bronze.crm_cust_info
where cst_key != trim(cst_key)
or cst_firstname != trim(cst_firstname)
or cst_lastname != trim(cst_lastname)


/*Test for table 2: bronze.crm_prd_info  */
--Check for nulls and duplicates in primary key
select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null;

--check for unwanted spaces

select prd_key
from bronze.crm_prd_info
where prd_key != trim(prd_key)

select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)

--check for nulls or negative values in product cost
select prd_id,prd_cost
from bronze.crm_prd_info
where prd_cost <0 or prd_cost is null


--normalization of prd_line
select distinct(prd_line)
from bronze.crm_prd_info

--check for invalid date orders
select * from bronze.crm_prd_info
where prd_start_date > prd_end_date


/*Test for table 3: bronze.crm_sales_details  */
select * from bronze.crm_sales_details

--check for nulls and dulicates in the primary key

select sls_ord_num, count(cast(substring(sls_ord_num,3,len(sls_ord_num)) as int)) as ord_count
from bronze.crm_sales_details
group by sls_ord_num
having count(cast(substring(sls_ord_num,3,len(sls_ord_num)) as int)) > 1 or sls_ord_num is null


--check for invalid dates in sls_due_dt
select 
	nullif(sls_due_dt,0) as sls_due_dt
from bronze.crm_sales_details
where sls_due_dt < sls_order_dt or sls_due_dt <= 0 or len(sls_due_dt) != 8


--check for invalid dates in sls_ord_dt
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt > sls_due_dt or sls_order_dt > sls_ship_dt

--check for data consistency : sales = quantity * price
select sls_sales, sls_quantity, sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
		or sls_sales is null
		or sls_quantity is null
		or sls_price is null
		or sls_sales <= 0
		or sls_quantity <= 0
		or sls_price <= 0

/*Test for table 4: bronze.erp_cust_az12  */
--identify out of range dates
select bdate 
from bronze.erp_cust_az12
where bdate > getdate()
--data standardization
select distinct gen
from bronze.erp_cust_az12

/*Test for table 5: bronze.erp_loc_a101  */
--data standarization and consistency
select distinct cntry
from bronze.erp_loc_a101


/*Test for table 6: bronze.erp_px_cat_g1v2  */
select * from bronze.erp_px_cat_g1v2

--check for unwanted spaces
select *
from bronze.erp_px_cat_g1v2
where cat != trim(cat)
	or subcat != trim(subcat)
	or maintenance != trim(maintenance)

--check for data standardization and consistency
select distinct maintenance
from bronze.erp_px_cat_g1v2