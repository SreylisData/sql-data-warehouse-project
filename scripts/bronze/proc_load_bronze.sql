/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CVS files.
	It performs the follwoing actions:
	- Truncates the bronze tables before loading data.
	- Uses the 'COPY' command to load data from CSV files to bronze tables.

Parameters:
	None. This stored procedure does not accept any parameters or return any values.

Usage Example:
	CALL bronze.load_bronze()
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze
(
)
LANGUAGE plpgsql 
AS $BODY$
	BEGIN
		TRUNCATE TABLE bronze.crm_cust_info;
		COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date) 
		FROM '/private/tmp/cust_info.csv' WITH (FORMAT CSV, HEADER TRUE);

		TRUNCATE TABLE bronze.crm_prd_info;
		COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt) 
		FROM '/private/tmp/prd_info.csv' WITH (FORMAT CSV, HEADER TRUE); 

		TRUNCATE TABLE bronze.crm_sales_details;
		COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price) 
		FROM '/private/tmp/sales_details.csv' WITH (FORMAT CSV, HEADER TRUE);

		TRUNCATE TABLE bronze.erp_cust_az12;
		COPY bronze.erp_cust_az12 (cid, bdate, gen) 
		FROM '/private/tmp/CUST_AZ12.csv' WITH (FORMAT CSV, HEADER TRUE);

		TRUNCATE TABLE bronze.erp_loc_a101;
		COPY bronze.erp_loc_a101 (cid, cntry) 
		FROM '/private/tmp/LOC_A101.csv' WITH (FORMAT CSV, HEADER TRUE);

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance) 
		FROM '/private/tmp/PX_CAT_G1V2.csv' WITH (FORMAT CSV, HEADER TRUE);
	end; 
$BODY$

CALL bronze.load_bronze()
