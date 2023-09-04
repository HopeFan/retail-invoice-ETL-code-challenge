from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import isnan, isnull
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

def transform_data(account_df, invoice_line_items_df, invoice_df, sku_df):

    """
    Transform the data by joining tables and calculating new columns
    """
    # join invoice table with account table
    account_invoice_df = invoice_df.join(account_df, "account_id")

    # join sku table with invoice_line_items table
    sku_invoice_line_items_df = sku_df.join(invoice_line_items_df, "item_id")

    # join account_invoice_df with sku_invoice_line_items_df
    join_all = account_invoice_df.join(sku_invoice_line_items_df, "invoice_id") \
        .select("invoice_id", "account_id", "date_issued", "payment_dates", "item_cost_price", "item_retail_price", "quantity",'joining_date')

    aggregation_df = join_all \
        .withColumn("acct_age", datediff(col("date_issued"), col("joining_date"))) \
        .withColumn("num_inv_120d", count(when((datediff(current_date(), col("date_issued")) <= 120), col("invoice_id"))).over(Window.partitionBy("account_id"))) \
        .withColumn("cum_tot_inv_acct", count(col("invoice_id")).over(Window.partitionBy("account_id").orderBy("date_issued").rowsBetween(Window.unboundedPreceding, Window.currentRow))) \
        .withColumn("inv_total", col("item_retail_price") * col("quantity")) \
        .groupBy("invoice_id", "account_id") \
        .agg(sum("inv_total").alias("inv_total"), count("quantity").alias("inv_items"), max("acct_age").alias("acct_age"), max("num_inv_120d").alias("num_inv_120d"), max("cum_tot_inv_acct").alias("cum_tot_inv_acct"))

    is_late_df = join_all.withColumn("is_late", when(datediff(col("payment_dates"), col("date_issued")) > 30, 1).otherwise(0))

    final_df = aggregation_df.join(is_late_df, ["invoice_id", "account_id"], "inner").dropDuplicates(["invoice_id", "account_id"])

    return final_df

def store_data(df, file_format):
    """
    store data as patquet file 
    """
    try:
        df.coalesce(1).write.format(file_format).option("compression", "snappy").save(os.environ.get('OUTPUT_FEATURES'))
        print("Data stored successfully.")
    except Exception as e:
        print("Error:", e) 


if __name__ == "__main__":
    # Create a SparkSession
    spark = create_spark_session()


    # Extrad data from four tables (E)
    account_df = read_table_from_csv(spark, os.environ.get('ACCOUNTS'), m_line = True)
    invoice_line_items_df = read_table_from_csv(spark, os.environ.get('INVOICE_LINE_ITEMS'))
    invoice_df = read_table_from_csv(spark,os.environ.get('INVOICES'))
    sku_df = read_table_from_csv(spark, os.environ.get('SKUS'))

    # Transform the data (T)
    joined_df = transform_data(account_df, invoice_line_items_df, invoice_df, sku_df)
    joined_df.show()

    # Load data into a parquet file for future use (L)
    #sdstore_data(joined_df,file_format='parquet')
