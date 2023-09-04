# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import Window
from dotenv import load_dotenv
import os

load_dotenv()

def create_spark_session():
    """
    Create or get an existing SparkSession
    """
    
    spark = SparkSession.builder.appName('ETL Pipeline') \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    return spark

def read_table_from_csv(spark, csv_path, m_line=False):
    """
    Read a table from a CSV file

    spark = create_spark_session()
    """
    try:
        table_df = spark.read.options(header='True', inferSchema='True', delimiter=',', multiLine = m_line).csv(csv_path)
    except Exception as e:
        print("Error reading CSV file:", e)
        return spark.createDataFrame([], StructType([]))
    return table_df


def return_null(df):
    """
    check null values in each column
    """
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    return null_counts

# Create a SparkSession
spark = create_spark_session()

#load the data
account_df = read_table_from_csv(spark, os.environ.get('ACCOUNTS'), m_line = True)
invoice_line_items_df = read_table_from_csv(spark, os.environ.get('INVOICE_LINE_ITEMS'), m_line = True)
invoice_df = read_table_from_csv(spark, os.environ.get('INVOICES'), m_line = True)
sku_df = read_table_from_csv(spark, os.environ.get('SKUS'), m_line = True)

# get some info out of data
account_df.printSchema()
df = return_null(account_df)
df.show()

invoice_line_items_df.printSchema()
df = return_null(invoice_line_items_df)
df.show()

invoice_df.printSchema()
df = return_null(invoice_df)
df.show()

sku_df.printSchema()
df = return_null(sku_df)
df.show()
