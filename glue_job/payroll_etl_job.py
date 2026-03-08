from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys

# Initialize Spark and GlueContext
sc= SparkContext()
glueContext=GlueContext(sc)
spark = glueContext.spark_session
#Job Initialization
args= getResolvedOptions(sys.argv,["JOB_NAME"])
job=Job(glueContext)
job.init(args["JOB_NAME"], args)

#input / output paths:
#input_path ="s3://payroll-raw-dataset/"
#output_path="s3://payroll-processed-dataset/"
# Read csv file:
df=spark.read \
   .option("header", "true") \
   .option("inferSchema", "true") \
   .csv("s3://payroll-raw-dataset/Salaries.csv")
print(df.columns)
#clean column names
df=df.toDF(*[c.strip().replace("","") for c in df.columns])
# Enforce Schema (Convert numeric columns)
column_dtypes = {"Id": "int",
                 "BasePay": "double",
                 "OvertimePay": "double",
                 "OtherPay": "double",
                 "Benefits": "double",
                 "TotalPay": "double",
                 "TotalPayBenefits": "double",
                 "Year": "int"}

for column_name, dtype in column_dtypes.items():
    df = df.withColumn(column_name,col(column_name).cast(dtype))
# Remove invalid salary rows
df = df.filter(col("TotalPayBenefits").isNotNUll())
# Write parquet partitioned by Year
df.write.mode("overwrite").format("parquet").save("s3://payroll-processed-dataset/processed/")
job.commit()
