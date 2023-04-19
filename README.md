# Star Project Simulation

# Business Requirement
<b>
 A multinational company wants to create a central storage repository (i.e. Data Lake) that will hold big data in raw as well as processed format. 
 This data will be further consumed by different users for a variety of business use cases. 
 As a data engineer, you need to create an end-to-end ETL pipeline that will first ingest raw data from the landing zone to a raw zone in an as-is format. 
 Then, the data will be processed and moved to a staging zone. The processing will include masking and cast transformation of some fields. 
 The name of the fields that will be transformed will be read from a configuration file. 
 The pipeline should be triggered on the upload of raw data in the landing zone. 
 Finally, create a lookup dataset for unmasked and masked data with SCD2 type implementation. </b>

# Dataset Description

### **1. Actives Dataset**
| Column Names | Data Type |Transformations | Partition Columns|
|--------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|advertising_id| string | None|No|
|city |string|None|No|
|location_category |string|None|No|
|location_granularities|string|None|No|
|location_source |array[string]|Convert to a comma-separated string|No|
|state| string|None|No|
|timestamp|bigint|None|No|
|user_id |string|None|No|
|user_latitude| double|convert to decimal with 7 precision|No|
|user_ longitude|double|convert to decimal with 7 precision|No|
|month |string|None|Yes|
|date |date|None|Yes|

### **2. Viewership Dataset**
| Column Names | Data Type | Partition Columns| Transformations|
|--------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|advertising_id|	string	|	No|None|	 
|channel_genre|string|No|None|	 
|channel_name|string|No|None|	 
|city|string|	No|None|	 
|device|string|No|None|	 
|device_type|string|No|None	 
|duration|integer|No|None|	 
|grid_id|string|No|None|	 
|language|string|No|None|	 
|location_category|string|No|None|	
|location_granularities|string|No|None|	 
|location_source|Array[String]|No|Convert to a comma-separated string|
|record_timestamp|bigint|No|None|	 
|show_genre|string|No|None|	 
|show_name|string|No|None|	 
|state|string|No|None|	 
|user_lat|double|No|Convert to decimal with 7 precision|
|user_long|double|No|Convert to decimal with 7 precision|
|month|string|Yes|None	 
|date|date|Yes|None 

# Project Outline
 
 ### Understood the business requirement and created a high-level architecture on AWS
     -> Identified the appropriate AWS services
     -> Understand the data schema
     -> Created a high-level architectural diagram using draw.io 
     -> The suggested solution was to be scalable and cost-effective
 ### Create S3 buckets for Data Lake
     -> Created three S3 buckets for landing zone, raw zone, and staging zone.
     -> Enabled appropriate life-cycle management on these buckets.
     -> Read & Write access for raw bucket should be limited to a service account for programmatic access only.Used IAM
     -> Enabled versioning on these buckets
     -> Prepared sample data in parquet format by referring to the schema.
### Create an EMR-Spark Job to perform data transformation
     -> Created a spark job using the programming language you learned.
     -> This spark job will read data from the raw zone and after the transformation put the data in the staging zone.
     -> Implemented the logic to mask critical fields 
     -> Implemented The logic to cast some fields.
     -> Created an EMR cluster that will be used to submit this spark job.
     -> Spark configuration should be configurable. The configuration should be read from a file.
     -> Manually executed the spark job.
     -> The EMR cluster should terminate after the job execution is done. Used cost-saving measures.
 4) Implemented Livy to interact with an EMR cluster over a REST interface
 5) Setup Airflow on EC2 using putty terminal
 6) Create a high-level DAG in Airflow
 7) Create Lambda function to trigger DAG
 8) Improve DAG by adding validations & lookup data set 
 9) Implement a basic deployment pipeline 
 10)Peer review of code, perform unit testing, and document the results
