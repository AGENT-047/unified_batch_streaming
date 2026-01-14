import os
import shutil
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col,lit,when,row_number,from_json, col, to_date, timestamp_seconds
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType
import sys

# os.environ["INPUT_DIR"] = "/opt/data/input/user"
# os.environ["ARCHIVE_DIR"] = "/opt/data/archive/user"
# os.environ["WAREHOUSE_DIR"] = "/opt/data/warehouse/user"



WAREHOUSE_DIR = "/opt/data/warehouse"





def get_spark_session():
    """
    Configures Spark to use the Local Filesystem (Host Machine) as the Warehouse
    """
    try:
        SparkSession.getActiveSession().stop()
    except:
        pass

    # 2. Define a custom catalog name (e.g., 'local_iceberg')
    catalog_name = "local_iceberg"

    return SparkSession.builder \
        .appName("HostWriteIceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", WAREHOUSE_DIR) \
         \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", WAREHOUSE_DIR) \
        \
        .config("spark.sql.defaultCatalog", catalog_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def user_dimensional_table_ingestion(param_table_name):

    # INPUT_DIR = "/opt/data/input/user"
    # ARCHIVE_DIR = "/opt/data/archive/user"
    # TABLE_NAME = "user_table"

    INPUT_DIR = f"/opt/data/input/{param_table_name}"
    ARCHIVE_DIR = f"/opt/data/archive/{param_table_name}"
    TABLE_NAME = f"{param_table_name}_table"

    spark = get_spark_session()

#     spark.sql(f"""  
#     CALL spark_catalog.system.expire_snapshots(
#     table => '{TABLE_NAME}',
#     older_than => TIMESTAMP '2026-01-01 00:00:00',
#     retain_last => 1
# )
#     """)
    # sys.exit(0)

    
    spark.sql(f"""
        delete from {TABLE_NAME} where country_code='IN'
    """)


    
    # Check for files
    if not os.path.exists(INPUT_DIR):
        print(f"Error: Input directory {INPUT_DIR} not found inside container.")
        return

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".json")]
    
    if not files:
        print("No json files found in input_data folder.")
        return

    print(f"Processing {len(files)} file(s)...")

    # 2. Read Data
    schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("created_at", TimestampType(), True),
    StructField("country_code", StringType(), True),
    StructField("device", 
                StructType([
                        StructField("os", StringType(), True),
                        StructField("version", IntegerType(), True)]) , True),
    StructField("_corrupt_record", StringType(), True) # Field to store corrupt records
])

    raw_df1 = spark.read.json("file://" +INPUT_DIR)
    raw_df1.show(5)

    raw_df = spark.read.option("header", "true") \
    .schema(schema) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json(INPUT_DIR)

    raw_df.persist()
    raw_df.show()
    print(f"raw_df count is {raw_df.count()} and filtered raw_df count is {raw_df.filter(col('_corrupt_record').isNull()).count()}")
    raw_df.unpersist()
    # sys.exit(0)

    raw_df=raw_df.filter(col("_corrupt_record").isNull())
    raw_df=raw_df.withColumn("country_code",when(col("country_code").isNull(),lit("UNKNOWN")).otherwise(col("country_code")))

    # 3. Deduplicate (Keep latest created_date per ID)
    window_spec = Window.partitionBy("user_id").orderBy(col("created_at").desc_nulls_last())
    deduped_df = raw_df.withColumn("rn", row_number().over(window_spec)) \
                       .filter("rn == 1").drop("rn")
    

    temp_table_name=f"incoming_data_{table_name}"
    deduped_df.createOrReplaceTempView(temp_table_name)



    # 4. Create Iceberg Table (if not exists)
    # This creates the folder structure inside ./warehouse on your Host
  

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            user_id STRING,
            created_at TIMESTAMP,
            country_code STRING,
            device STRUCT<os: STRING, version: INT>
        ) USING iceberg
    """)


    spark.sql(f"""
        MERGE INTO {TABLE_NAME} target
        USING {temp_table_name} source
        ON target.user_id = source.user_id
        WHEN MATCHED AND source.created_at > target.created_at THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    
    print("Merge Complete. Data written to Host ./warehouse folder.")

    # 6. Archive Files (Move from Input to Archive)
    # for file in files:
    #     src = os.path.join(INPUT_DIR, file)
    #     dst = os.path.join(ARCHIVE_DIR, file)
        
    #     # Move the file
    #     shutil.move(src, dst)
    #     print(f"Moved {file} to archive.")



def countries_dimensional_table_ingestion(param_table_name):

    INPUT_DIR = f"/opt/data/input/{param_table_name}"
    ARCHIVE_DIR = f"/opt/data/archive/{param_table_name}"
    TABLE_NAME = f"{param_table_name}_table"

    spark = get_spark_session()


    
    # spark.sql(f"""
    #     delete from {TABLE_NAME} where country_code='IN'
    # """)

    # sys.exit(0)

    
    # Check for files
    if not os.path.exists(INPUT_DIR):
        print(f"Error: Input directory {INPUT_DIR} not found inside container.")
        return

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".csv")]
    
    if not files:
        print("No json files found in input_data folder.")
        return

    print(f"Processing {len(files)} file(s)...")

    # 2. Read Data
    schema = StructType([
    StructField("country_code", StringType(), False),
    StructField("country_name", StringType(), True),
    StructField("_corrupt_record", StringType(), True) # Field to store corrupt records
])

    raw_df1 = spark.read.csv("file://" +INPUT_DIR)
    raw_df1.show(5)

    raw_df = spark.read.option("header", "true") \
    .schema(schema) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv(INPUT_DIR)

    raw_df.persist()
    raw_df.show()
    print(f"raw_df count is {raw_df.count()} and filtered raw_df count is {raw_df.filter(col('_corrupt_record').isNull()).count()}")
    raw_df.unpersist()
    # sys.exit(0)

    raw_df=raw_df.filter(col("_corrupt_record").isNull())
    # raw_df=raw_df.withColumn("country_code",when(col("country_code").isNull(),lit("UNKNOWN")))

    # 3. Deduplicate (Keep latest created_date per ID)
    # window_spec = Window.partitionBy("user_id").orderBy(col("created_at").desc_nulls_last())
    # deduped_df = raw_df.withColumn("rn", row_number().over(window_spec)) \
    #                    .filter("rn == 1").drop("rn")

    deduped_df=raw_df
    
    temp_table_name=f"incoming_data_{table_name}"
    deduped_df.createOrReplaceTempView(temp_table_name)

    # 4. Create Iceberg Table (if not exists)
    # This creates the folder structure inside ./warehouse on your Host
  

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            country_code STRING,
            country_name STRING
        ) USING iceberg
    """)


    spark.sql(f"""
        MERGE INTO {TABLE_NAME} target
        USING {temp_table_name} source
        ON target.country_code = source.country_code
        WHEN MATCHED AND source.country_name <> target.country_name THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    
    print("Merge Complete. Data written to Host ./warehouse folder.")

    print("showing data.")

    spark.sql(f"""
        select * from {TABLE_NAME} 
    """).show(truncate=False)

    # 6. Archive Files (Move from Input to Archive)
    # for file in files:
    #     src = os.path.join(INPUT_DIR, file)
    #     dst = os.path.join(ARCHIVE_DIR, file)
        
    #     # Move the file
    #     shutil.move(src, dst)
    #     print(f"Moved {file} to archive.")


def events_stream_ingestion(param_table_name):

    KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
    KAFKA_TOPIC = "events_topic"
    # CHECKPOINT_DIR = os.path.join(WAREHOUSE_DIR, "checkpoints", "events_3")
    OUTPUT_PATH = os.path.join(WAREHOUSE_DIR, "events_data")  

    CHECKPOINT_DIR = "/tmp/checkpoints/events_clean_start"

    # --- 2. ADD THIS CLEANUP BLOCK HERE ---
    # This deletes the corrupt checkpoint every time you run the script
    if os.path.exists(CHECKPOINT_DIR):
        print(f"🧹 Deleting old checkpoint at {CHECKPOINT_DIR}...")
        shutil.rmtree(CHECKPOINT_DIR)


    print(CHECKPOINT_DIR, OUTPUT_PATH)      

    print("starting stream")
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Define Schema for incoming JSON
    schema = StructType() \
        .add("event_id", StringType()) \
        .add("user_id", StringType()) \
        .add("event_type", StringType()) \
        .add("event_timestamp", TimestampType())

    # 2. Read Stream from Kafka
    # We load the "kafka" format. The "value" column contains our JSON data.
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Parse JSON and Extract Fields
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # 4. Add 'date' column for Partitioning
    processed_df = parsed_stream.withColumn("date", to_date(col("event_timestamp")))

    # 5. Deduplication
    # Important: For streaming deduplication, we MUST use a watermark.
    # This tells Spark: "Drop duplicates, but only remember data for 10 minutes."

    # deduped_df = processed_df \
    #     .withWatermark("event_timestamp", "10 minutes") \
    #     .dropDuplicates(["event_id"])

    deduped_df = processed_df.coalesce(1)

# for showing purpose not to write the data
    query = deduped_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()


    query = deduped_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .partitionBy("date") \
        .start()
    
    print("data write to table compleed")
    

    print(f"Streaming started. Writing to {OUTPUT_PATH}...")
    query.awaitTermination()

if __name__ == "__main__":

    job_type=sys.argv[1]
    table_name=sys.argv[2]

    print(job_type,table_name)
    

    # match job_type:
    #     case "stream":
    #         print("running stream job")
    #     case "batch":
    #         match table_name:
    #             case "user":
    #                 print(f"running {table_name} table batch job")
    #             case "countries":
    #                 countries_dimensional_table_ingestion(table_name)
    #                 print(f"running {table_name} table batch job")
    #             case _:  
    #                 print(f"Unexpected table_name {table_name}")
                    
    #     case _:  
    #         print(f"Unexpected job_type {job_type}")
    
    if job_type == "streaming":
        events_stream_ingestion(table_name)
    elif job_type == "batch":
        if table_name == "user":
            print(f"running {table_name} table batch job")
            user_dimensional_table_ingestion(table_name)  

        elif table_name == "countries":
            print(f"running {table_name} table batch job")
            countries_dimensional_table_ingestion(table_name)  

        else:
            print(f"Unexpected table_name {table_name}")
    else:
        print(f"Unexpected job_type {job_type}")

