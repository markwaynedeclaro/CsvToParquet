  
# CSV to Parquet Converter
  
The application performs the following:  
- Uses Spring Scheduler. Upon triggering, the default delay start time is 5 seconds. This is configurable in ***application.properties*** file
- This application uses Spring Profiling feature. Make sure that the value of spring.profiles.active is **dev** when running the application.
- Use provided AWS credentials to download the zip file from your s3 bucket, and extract csv files from it.  
- Read the CSV, search and extract lines which contain the word "ellipsis" (in any field), then save into a new CSV File
- A schema file will be created per CSV file. If the CSV file has a header row, it will use that as input for schema file creation, else a default schemma file will be generated based from the number of columns of the CSV file.  
- The new CSV file and the schema file will be used to create a corresponding Parquet file.  
- The Parquet file will be named same as that of the CSV (e.g. matching lines in news.csv → news.parquet)  
- Once all conversions are completed, compress the output files into a single zip file ( **output.zip** ) 
- Upload the output zip file in the same S3 bucket  
  
  
## MVN Run Configuration 
  
*mvn spring-boot:run -Dspring-boot.run.arguments="--key.id=<YOUR_ACCESS_KEY_ID> --access.key=<YOUR_SECRET_ACCESS_KEY> --bucket.name=<YOUR_BUCKET_NAME> --region=<YOUR_REGION>"*
  
  
## Running Unit Test 
Make sure to do the following first before triggering unit tests:
- This application uses Spring Profiling feature. Make sure that the value of ****spring.profiles.active**** is **test** when running unit tests.
- In file **PollingTimerConfig.java**, comment out line #21 (//@Scheduled ...)


  
