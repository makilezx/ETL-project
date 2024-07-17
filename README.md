<a href="#"><p align="left">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/python-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/docker-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/postgressql-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/apacheairflow-dark.svg" width="50">
</p></a>


# ETL pipeline project

This repository contains the code that handles end-to-end data engineering solution, from data extraction, manipulation and orchestration to dashboard creation. 

Specifically, it uses Python scripts for the ETL process and Apache Airflow for orchestration, while the Postgres database acts as a warehouse where pre-processed data is ingested. Moreover, the whole pipeline is _dockerized_ with the aim of providing a modular, isolated environment. Finally, Metabase handles visualizations and dashboards. 

## ETL diagram
![aa1](https://github.com/user-attachments/assets/803d2ccd-c008-47ef-9d4a-22e23590d428)


## Project structure

<pre>
│   docker-compose.yml  <- Docker config/dependencies
│   Dockerfile          <- Dockerfile
│   init.sql            <- SQL code for Postgres DB initialization
│   requirements.txt    <- The requirements file
│
├───dags
│       etl_dag.py      <- Airflow DAG file
│
├───data
│       source.xlsx     <- Source data
│
├───scripts            
│       extract.py      <- Python code for data extraction 
│       load.py         <- Python code for data loading 
│       transform.py    <- Python code for data transformation 
│       validate_extraction.py        <- Python code for validation of data extraction 
│       validate_transformation.py    <- Python code for validation of data transformation 
│
└───sql_scripts
        analytics_queries.sql    <- SQL queries written for analytics purposes 
</pre>

#### Python scripts
* `pandas` is extensively used as well as `numpy`
* Custom logging is implemented through the pipeline using `logging` library. Since the pipeline was initially developed on the local machine, it proved extremely useful.
* `black` is used for formatting Python scripts.

##### Example picture: Airflow DAG
![aa6](https://github.com/user-attachments/assets/570bd127-b364-41bf-b0aa-d7139a3defce)



#### Data format(s)
* The goal was not only to ensure a smooth processing flow, where each stage in the pipeline depends on files produced in the previous stage, but also to maintain the raw, staging, and source files in suitable formats for storage purposes.
    * Specifically, the first stage (extraction) relies on retrieving data from various Excel sheets/files and converting it into a usable format for subsequent processing. Extracted data is put in a .json file and this decision is made due to the mixed and unspecified data types in raw data.
    * Data processing is performed in the transformation stage, where data is validated and cleaned before it’s used downstream. As a result, processed data is stored in .parquet files. 

#### Visualization
* Metabase is running in a separate container. SQL queries used for analytics purposes as well as for dashboard creation can be found in `sql_scripts`

##### Example picture: part of the dashboard created with SQL and Metabase
![aa2](https://github.com/user-attachments/assets/a9385ec6-c576-48ba-9396-6299e50f557d)
