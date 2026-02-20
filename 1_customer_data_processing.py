# Databricks notebook source
# DBTITLE 1,import func
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,utilities_full_path
# MAGIC %run /Workspace/Users/venkateshvkt10@gmail.com/Data-engineer/1_setup/Utilities

# COMMAND ----------

# DBTITLE 1,utilities_schema_print
print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

# DBTITLE 1,creating_catalog_and_data_sources_name
dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")



# COMMAND ----------

# DBTITLE 1,Use_widget_to_call_S3_Bucket_data_sources
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3a://sportsbar-dm-vkt/{data_source}/*.csv"

print(base_path)

# COMMAND ----------

# DBTITLE 1,Data_Frame_column_creation
df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

# DBTITLE 1,10 row of data_frame
display(df.limit(10))


# COMMAND ----------

# DBTITLE 1,schema_data_type of data_frame
# print check data type
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Overwrite_changes_in_bronze
df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **_Silver processing_**

# COMMAND ----------

# DBTITLE 1,Check_the_catalog_from_bronze_schema
df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

# DBTITLE 1,Bronze_table_schema
# let look schema table of bronze
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### -1 -----------Transformation -- Duplicates --------------

# COMMAND ----------

# DBTITLE 1,Find_duplicates
# find the dulipcate from bronze schema table where column name is customer_id
df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

# DBTITLE 1,Drop_duplicate_and _its_count
print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **### _### ** 2: Trim spaces in customer `name`**_**

# COMMAND ----------


display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

# DBTITLE 1,Trim_values
## remove those trim values

df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

# DBTITLE 1,Check_trim_function_again_for space in customer name
#Check_trim_function_again_for space in customer name
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  - 3: Data Quality Fix: Correcting City Typos**

# COMMAND ----------

# DBTITLE 1,Select_distinct_city_for transformation
df_silver.select('city').distinct().show()

# COMMAND ----------

# DBTITLE 1,typos city→ correct names
# # typo dictionary
# city_typos = {
#     'Bengaluru': ['Bengaluruu', 'Bengaluruu', 'Bengalore'],
#     'Hyderabad': ['Hyderabadd', 'Hyderbad'],
#     'New Delhi': ['NewDelhi', 'NewDheli', 'NewDelhee']
# }

# typos → correct names
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}


allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
         .when(F.col("city").isin(allowed), F.col("city"))
         .otherwise(None)
    )
)

# COMMAND ----------

# DBTITLE 1,check_city_validation_done
# Sanity check
df_silver.select('city').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **4: Fix Title-Casing **Issue****

# COMMAND ----------

# DBTITLE 1,Finding unique customer name  
df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# DBTITLE 1,customer name is null then None and all case sensitive
# Title case fix
df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
     .otherwise(F.initcap("customer_name"))
)

# COMMAND ----------

# DBTITLE 1,check validation
# sanity check

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### - 5: Handling missing _cities_

# COMMAND ----------

# DBTITLE 1,Find out customer who does not have city
df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,finding customer name where (IN) mentioned name
null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Assigning the city to them and stored in data frame
# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

# DBTITLE 1,Used the city data frame to assign city as per the data frame created
df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")   # Replace null with fixed city
    )
    .drop("fixed_city")
)

# COMMAND ----------

# DBTITLE 1,Now checking the city o chose customer who previously not have and now fixed
#Now checking the city o chose customer who previously not have and now fixed
null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### - 6: Convert customer_id to string

# COMMAND ----------

# DBTITLE 1,converting dat type of customer id to string
df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC **Standardizing Customer Attributes to Match Parent Company Data Model**

# COMMAND ----------

# DBTITLE 1,Three additional column add as per the requirement  
df_silver = (
    df_silver
    # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    
    # Static attributes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

# DBTITLE 1,check result
display(df_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,overwrite_df_silver_table_with_column_updation
df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # _Gold_ processing

# COMMAND ----------

# DBTITLE 1,Required column from silver to gold
df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")


# take req cols only
# "customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

# DBTITLE 1,rewrite the gold data update
df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging Data source with parent

# COMMAND ----------

# DBTITLE 1,merging child customer to parent customer
delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

# DBTITLE 1,transformation done upset means insert nd update
delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()