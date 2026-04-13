Data Catalog for Gold Layer
------------------
Overview
------------

The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of dimension tables and fact tables for specific business metrics.


gold.dim_customers
------------------
Purpose: Stores customer details enriched with demographic and geographic data.


Columns:
-



<img width="954" height="418" alt="Screenshot 2026-04-13 at 1 30 06 PM" src="https://github.com/user-attachments/assets/4be59e25-aa4c-46b0-9ad6-9d52c5cc5013" />


gold.dim_products
---------------------
Purpose: Provides information about the products and their attributes.

Columns:
-

<img width="1019" height="505" alt="Screenshot 2026-04-13 at 1 33 38 PM" src="https://github.com/user-attachments/assets/5078ff77-4b1d-468a-86c4-dbcc182c40ab" />

-----------


gold.fact_sales
--

Purpose: Stores transactional sales data for analytical purposes.

Columns:
-

<img width="941" height="379" alt="Screenshot 2026-04-13 at 1 35 47 PM" src="https://github.com/user-attachments/assets/5565aebe-b29e-43f3-9629-a5616d2e76e8" />

