
/*
SCRIPT PURPOSE:
This script create views for the Gold Layer in data warehouse
The gold layer represents the final dimension and fact tables (Star Schema)

Each view performs transformations and combines data from Silver Layer to 
produce a clean, enriched and business-ready dataset.

USAGE:
- These views can be queried directly for analytics and reporting.
*/

-- ===================================================================
-- Create Dimension: gold.dim_customers
-- ===================================================================
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cloc.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(cbd.gen, 'n/a')
	END AS gender,
	cbd.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 cbd
ON ci.cst_key = cbd.cid
LEFT JOIN silver.erp_loc_a101 cloc
ON ci.cst_key = cloc.cid

-- ===================================================================
-- Create Dimension: gold.dim_products
-- ===================================================================
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,
	pi.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pi.prd_cost AS cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pi.cat_id = pc.id
WHERE prd_end_dt IS NULL

-- ===================================================================
-- Create Fact: gold.fact_sales
-- ===================================================================
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	p.product_key,
	c.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_prd_sales_details sd
LEFT JOIN gold.dim_products p
ON sd.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers c
ON sd.sls_cust_id = c.customer_id
