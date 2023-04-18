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

