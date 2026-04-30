import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Logging the setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

# Glue context setup
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuration
BUCKET         = "flight-pipeline-vishara"
RAW_PATH       = f"s3://{BUCKET}/raw/Clean_Dataset.csv"
PROCESSED_PATH = f"s3://{BUCKET}/processed/"

# Read the raw CSV from S3
logger.info("=== Step 1: Reading raw CSV from S3 ===")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)
logger.info(f"Raw record count: {df.count()}")
df.printSchema()

# Handling duplicates
logger.info("=== Step 2: Removing duplicates ===")
before = df.count()
df = df.dropDuplicates()
after = df.count()
logger.info(f"Duplicates removed: {before - after} rows")

# Handling missing values
logger.info("=== Step 3: Handling missing values ===")
# Log the null counts per column
for col_name in df.columns:
    null_count = df.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        logger.info(f"  Column '{col_name}' has {null_count} nulls")

# Dropping rows where critical columns are null
critical_cols = ["airline", "source_city", "destination_city", "price", "duration", "stops"]
df = df.dropna(subset=critical_cols)

# Fill non-critical null values with defaults
df = df.fillna({
    "flight": "UNKNOWN",
    "departure_time": "Unknown",
    "arrival_time": "Unknown",
    "class": "Economy",
    "days_left": 0
})
logger.info(f"Record count after null handling: {df.count()}")

# Data type conversions
logger.info("=== Step 4: Data type conversions ===")
df = df.withColumn("duration",  F.col("duration").cast(DoubleType()))
df = df.withColumn("price",     F.col("price").cast(IntegerType()))
df = df.withColumn("days_left", F.col("days_left").cast(IntegerType()))

# Standardizing the data
logger.info("=== Step 5: Standardizing categorical columns ===")
# Trim whitespace and title case all the string columns
string_cols = ["airline", "flight", "source_city", "departure_time",
               "stops", "arrival_time", "destination_city", "class"]
for col_name in string_cols:
    df = df.withColumn(col_name, F.initcap(F.trim(F.col(col_name))))

# Standardize stops column (zero as 0, one as 1, two_or_more as 2+)
df = df.withColumn("stops",
    F.when(F.lower(F.col("stops")) == "zero", "0")
     .when(F.lower(F.col("stops")) == "one",  "1")
     .when(F.lower(F.col("stops")).contains("two"), "2+")
     .otherwise(F.col("stops"))
)

# Handling corrupt data
logger.info("=== Step 6: Removing corrupt records ===")
before = df.count()
df = df.filter(F.col("price")    > 0)
df = df.filter(F.col("duration") > 0)
df = df.filter(F.col("days_left") >= 0)
logger.info(f"Corrupt records removed: {before - df.count()} rows")

# Data aggregation
logger.info("=== Step 7: Adding aggregated features ===")
# Average price per airline + class
avg_price_df = df.groupBy("airline", "class").agg(
    F.round(F.avg("price"), 2).alias("avg_price_airline_class"),
    F.count("*").alias("flight_count")
)
df = df.join(avg_price_df, on=["airline", "class"], how="left")

# Average price per route
route_avg_df = df.groupBy("source_city", "destination_city").agg(
    F.round(F.avg("price"), 2).alias("avg_price_route")
)
df = df.join(route_avg_df, on=["source_city", "destination_city"], how="left")

logger.info(f"Final record count: {df.count()}")
df.printSchema()

# Write the processed data to S3 as Parquet
logger.info("=== Step 8: Writing processed data to S3 ===")
df.write.mode("overwrite").parquet(PROCESSED_PATH)
logger.info(f"Data written to {PROCESSED_PATH}")

# Completed message
logger.info("=== ETL job completed successfully ===")
job.commit()
