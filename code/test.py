import main_script
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col,lit,when,row_number,from_json, col, to_date, timestamp_seconds
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType

spark=main_script.get_spark_session()

deduped_df=spark.read.parquet("/opt/data/warehouse/events_data")
deduped_df.createOrReplaceTempView("temp_table")

# spark.sql(f"""
#         select * from temp_table
#     """).show(truncate=False)

print("total count in the table is")
spark.sql(f"""
        select count(1) from temp_table
    """).show(truncate=False)




