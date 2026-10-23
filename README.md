\# E-Commerce Sales Analytics Using Databricks



\## Project Overview



This project demonstrates an end-to-end data engineering workflow using \*\*Databricks\*\* and the \*\*Medallion Architecture\*\* to process e-commerce sales data and prepare it for analytics.



The project organizes data processing into Bronze, Silver, and Gold layers and includes dimension processing, fact processing, and SQL-based data transformations.



\## Architecture



The project follows the Medallion Architecture:



\- \*\*Bronze Layer:\*\* Initial data ingestion and storage.

\- \*\*Silver Layer:\*\* Data cleaning and transformation.

\- \*\*Gold Layer:\*\* Refined data prepared for analytics.



\## Project Structure



```text

project\_ecommerce/

├── 1\_medallion\_processing\_dim/

│   ├── 1\_dim\_bronze.py

│   ├── 2\_dim\_silver.py

│   └── 3\_dim\_gold.py

├── 2\_medallion\_processing\_fact/

│   ├── 1\_fact\_bronze.py

│   ├── 2\_fact\_silver.py

│   └── 3\_fact\_gold.py

├── 3\_etl\_workflow/

│   ├── denormalise\_table\_query.sql

│   └── denormalise\_table\_query.txt

├── 3\_gold\_views/

│   └── denorm\_view.sql

├── setup/

├── E-Commerce\_Sales\_Analytics\_Documentation.md

└── README.md

```



\## Key Components



1\. \*\*Dimension Processing:\*\* Processes dimension data through Bronze, Silver, and Gold layers.

2\. \*\*Fact Processing:\*\* Processes fact data through the Medallion layers.

3\. \*\*ETL Workflow:\*\* Contains SQL queries for data denormalization.

4\. \*\*Gold Views:\*\* Contains SQL for creating a denormalized analytical view.

5\. \*\*Documentation:\*\* Provides additional project details.



\## Technologies Used



\- Databricks

\- Python

\- SQL

\- Medallion Architecture

\- ETL and Data Transformation



\## Learning Outcomes



\- Understanding the Bronze, Silver, and Gold data layers.

\- Organizing dimension and fact data processing.

\- Working with SQL-based data transformations.

\- Preparing structured data for analytical use.



\## Repository



This project is maintained on GitHub for version control and project documentation.



