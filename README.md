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
