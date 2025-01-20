# Databricks notebook source
spark

# COMMAND ----------

storage_account = "ecommerceuday"
application_id = "20cf9c33-83dd-49a3-8ea0-f88b2aeeea7a"
directory_id = "241fecde-e271-4652-b277-a4a9067930f0"
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "EPl8Q~ALW~hP.RD0VWpUQy2KEBHdCLGmnj46Scfy")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC Reading Data

# COMMAND ----------

base_path = "abfss://data@ecommerceuday.dfs.core.windows.net/Bronze/"
olist_customers_dataset=base_path+"olist_customers_dataset.csv"
olist_geolocation_dataset=base_path+"olist_geolocation_dataset.csv"
olist_order_items_dataset=base_path+"olist_order_items_dataset.csv"
olist_order_reviews_dataset=base_path+"olist_order_reviews_dataset.csv"
olist_orders_dataset=base_path+"olist_orders_dataset.csv"
olist_products_dataset=base_path+"olist_products_dataset.csv"
olist_sellers_dataset=base_path+"olist_sellers_dataset.csv"
olist_order_payments_dataset=base_path+"olist_order_payments_dataset"

customers_df= spark.read.format("csv").option("header", "true").load(olist_customers_dataset)
geolocation_df = spark.read.format("csv").option("header", "true").load(olist_geolocation_dataset)
items_df = spark.read.format("csv").option("header", "true").load(olist_order_items_dataset)
reviews_df = spark.read.format("csv").option("header", "true").load(olist_order_reviews_dataset)
orders_df = spark.read.format("csv").option("header", "true").load(olist_orders_dataset)
products_df = spark.read.format("csv").option("header", "true").load(olist_products_dataset)
sellers = spark.read.format("csv").option("header", "true").load(olist_sellers_dataset)
payments = spark.read.format("csv").option("header", "true").load(olist_order_payments_dataset)

# COMMAND ----------

import pymongo


# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

# MAGIC %md
# MAGIC Connecting to MongoDB

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "d961z.h.filess.io"
database = "reddy_mudcreamdo"
port = "27017"
username = "reddy_mudcreamdo"
password = "a16ffc9f496dd73daeaa6cf021a88dae0ffc7cdb"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]


# COMMAND ----------

import pandas as pd
collection = mydatabase['product_categories']

mongo_data= pd.DataFrame(list(collection.find()))

# COMMAND ----------

mongo_data

# COMMAND ----------

# MAGIC %md
# MAGIC Cleaning the data

# COMMAND ----------

from pyspark.sql.functions import col,to_date,datediff,current_date

# COMMAND ----------

def clean_dataframe(df,name):
    print("cleaning"+name)
    return df.dropDuplicates().na.drop('all')

orders_df=clean_dataframe(orders_df,"orders")
customers_df=clean_dataframe(customers_df,"customers")
geolocation_df=clean_dataframe(geolocation_df,"geolocation")    
items_df=clean_dataframe(items_df,"items")
reviews_df=clean_dataframe(reviews_df,"reviews")
payments=clean_dataframe(payments,"payments")
products_df=clean_dataframe(products_df,"products")
sellers=clean_dataframe(sellers,"sellers")

display(orders_df)

# COMMAND ----------

#changing  the dates to to-date format 
orders_df=orders_df.withColumn("order_purchase_timestamp",to_date(col("order_purchase_timestamp"))).withColumn("order_approved_at",to_date(col("order_approved_at"))).withColumn("order_delivered_carrier_date",to_date(col("order_delivered_carrier_date"))).withColumn("order_delivered_customer_date",to_date(col("order_delivered_customer_date"))).withColumn("order_estimated_delivery_date",to_date(col("order_estimated_delivery_date")))
display(orders_df)

# COMMAND ----------

#calculating timediff and delays

orders_df=orders_df.withColumn("acutal_delivery_time",datediff(col("order_delivered_customer_date"),col("order_purchase_timestamp")))
orders_df=orders_df.withColumn("estimated_delivery_time",datediff(col("order_estimated_delivery_date"),col("order_purchase_timestamp")))
orders_df=orders_df.withColumn("delay",col("estimated_delivery_time")-col("acutal_delivery_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC Joinings all cvs or tables by using schema(Transformations)

# COMMAND ----------

orders_customers_df=orders_df.join(customers_df,orders_df.customer_id==customers_df.customer_id,"left")
orders_payments_df=orders_customers_df.join(payments,orders_customers_df.order_id==payments.order_id,"left")
orders_reviews_df=orders_payments_df.join(reviews_df,"order_id","left")
orders_items_df=orders_reviews_df.join(items_df,"order_id","left")
orders_products_df=orders_items_df.join(products_df,"product_id","left")
orders_sellers_df=orders_products_df.join(sellers,"seller_id","left")
final_df=orders_sellers_df.join(geolocation_df,orders_sellers_df.customer_zip_code_prefix==geolocation_df.geolocation_zip_code_prefix,"left")


# COMMAND ----------

# MAGIC %md
# MAGIC Changing pandas dataframe to spark data frame for mongo data

# COMMAND ----------

mongo_data.drop("_id",axis=1,inplace=True)

mongo_df=spark.createDataFrame(mongo_data)
display(mongo_df)

# COMMAND ----------

final_df= final_df.join(mongo_df,final_df.product_category_name==mongo_df.product_category_name,"left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Removing Dulicate columns

# COMMAND ----------

def remove_duplicate_columns(df):
    columns = df.columns

    x=set()
    columns_to_drop = []
    for column in columns:
        if column in x:
          columns_to_drop.append(column)
        else:
          x.add(column)

    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned

final_df=remove_duplicate_columns(final_df)


# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC writing back to AdlsGen 2 

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://data@ecommerceuday.dfs.core.windows.net/Silver")