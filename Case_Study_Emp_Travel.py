import google.cloud.logging
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

#configuring Cloud Logg for error detection
client=google.cloud.logging.Client()
client.setup_logging()
logger=logging.getLogger("Empolyee-Flight-Status-Pipeline")
spark=SparkSession.builder.appName("Employee-Flight").getOrCreate()

def logging_status(step,message,LEVEL="INFO"):
    logger_message=f"Step : {step} - {message}"
    if LEVEL=="WARNING":
        logger.warning(logger_message)
    elif LEVEL=="ERROR":
        logger.error(logger_message)
    else:
        logger.info(logger_message)

#configuring variable
GCS_DATA_PATH="gs://data-bucket-sharvik/sample data.csv"
GCS_TEMP_PATH="gs://data-bucket-temp/"
TABLE_ID="project-a069533b-6ad2-4767-b24.sample_data.employee_fly_status"
SCHEMA = StructType([
    StructField("Booking_Date", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Pax_Name", StringType(), True),
    StructField("Travel_Date", StringType(), True),
    StructField("Sector", StringType(), True),
    StructField("Airline", StringType(), True),
    StructField("Class", StringType(), True),
    StructField("Flown_Status", StringType(), True)
])

#Function to read data from GCS bucket
def read_data():
    logging_status("Data Ingestion",f"Reading Data from GCS BUCKET {GCS_DATA_PATH}")
    try:
        df=spark.read.schema(SCHEMA).csv(GCS_DATA_PATH,header=True)
        #df.show()
    except Exception as e:
        logging_status("Ingestion",f"Got Error while reading data {e}","ERROR")
    return df

#Function to Transform Data
def transform_data(df):
    logging_status("Data Transformation","Clean and standardize string column")
    try:
        df1=(df.withColumn("Pax_Name",F.trim(F.regexp_replace(F.col("Pax_Name"),r"(?i)^(MR\.?|MRS\.?)\s*", "")))
                .withColumn("Booking_Date",F.date_format(F.coalesce(F.to_date(F.col("Booking_Date"),"M/d/yyyy H:mm"),
                                                                   F.to_date(F.col("Booking_Date"),"M/d/yyyy") ),"dd-MM-yyyy"))
                .withColumn("Travel_Date",F.date_format(F.coalesce(F.to_date(F.col("Travel_Date"),"M/d/yyyy H:mm"),
                                                                   F.to_date(F.col("Travel_Date"),"M/d/yyyy")),"dd-MM-yyyy")))
        
        df2=(df1.withColumn("Airline",F.upper(F.trim(F.col("Airline"))))
                .withColumn("Airline",F.when(F.col("Airline")=="INDGO","INDIGO")
                                     .when(F.col("Airline")=="SPICEGET","SPICEJET")
                                     .when(F.col("Airline")=="AIR VITARA","AIR VISTARA")
                                     .otherwise(F.col("Airline"))))
        
        df3=(df2.withColumn("Flown_Status",F.when((F.col("Flown_Status").isNull())| (F.col("Flown_Status")==""),"UNKNOWN")
                                           .otherwise(F.upper(F.trim(F.col("Flown_Status")))))
                .withColumn("Flown_Status",F.when(F.col("Flown_Status").isin('FLOWN', 'RFND', 'REISSUED', 'UNKNOWN'),F.col("Flown_Status"))
                                           .otherwise("UNKNOWN")))
        
        df4=df3.dropDuplicates(["booking_date","department","pax_name","travel_date","sector"])
        
        df5=(df4.withColumn("Sector",F.when(F.col("Sector").contains("/"),F.split(F.col("Sector"),"/")[0])
                                       .otherwise(F.col("Sector")))
                .withColumn("Airline",F.when(F.col("Airline").contains("/"),F.split(F.col("Airline"),"/")[0])
                                       .otherwise(F.col("Airline")))) 
        
        
        
    except Exception as e:
        logging_status("Transformation 1",f"Got Error while trasnforming data {e}","ERROR")
    return df5
    
#Load to BigQuery
def load_to_BQ(transformed_df):
    logging_status("Load to BQ","Loading Data into Bigquery")
    try:
        (transformed_df.write
                    .format("bigquery")
                    .option("Table",TABLE_ID)
                    .option("TemporaryGcsBucket",GCS_TEMP_PATH)
                    .mode("append")
                    .save())
    except Exception as e:
        logging_status("Transformation 1",f"Got Error while trasnforming data {e}","ERROR")
        
    
    
    
if __name__=="__main__":
    df=read_data()
    transformed_df=transform_data(df)
    load_to_BQ(transformed_df)
    spark.stop()
    

