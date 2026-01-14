import os
import shutil
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col,lit,when,row_number,from_json, col, to_date, timestamp_seconds
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType
import sys
import main_script


def output_data(date_param):



    # INPUT_DIR = f"/opt/data/input/{param_table_name}"
    # ARCHIVE_DIR = f"/opt/data/archive/{param_table_name}"
    # TABLE_NAME = f"{param_table_name}_table"

    spark = main_script.get_spark_session()


    
    user_country_joinDF=spark.sql(f"""
        select a.user_id,b.country_name from user_table a left join 
              countries_table b on a.country_code=b.country_code
    """)

    user_country_joinDF.show()

    user_country_joinDF=user_country_joinDF \
    .withColumn("country_name",when(col("country_name").isNull(),lit("UNKNOWN")).otherwise(col("country_name")))

    user_country_joinDF.createOrReplaceTempView("user_country_joinDF")
    eventsDF=spark.read.parquet("/opt/data/warehouse/events_data")
    eventsDF.createOrReplaceTempView("eventsDF")

    events_joinDF=spark.sql(f"""
        select a.user_id,b.country_name,a.date,a.event_id
                            from eventsDF a left join user_country_joinDF b
                            on a.user_id= b.user_id and a.date=to_date({date_param},'yyyy-MM-dd')
                            """)
    print("count is ")
    print( events_joinDF.count())
    
    events_joinDF.createOrReplaceTempView("events_joindf_view")

    output_df=spark.sql(f"""
        select user_id,country_name,date,count(event_id)
                            from events_joindf_view 
                        group by user_id,country_name,date
                            """)
    
    output_df.show(truncate=False)
    print(output_df.count())
    
    output_df.write.partitionBy("date").option("mode","overwrite").parquet("/opt/data/warehouse/output_data")

if __name__ == "__main__":
    output_data("2026-01-14")

    


    

    # 6. Archive Files (Move from Input to Archive)
    # for file in files:
    #     src = os.path.join(INPUT_DIR, file)
    #     dst = os.path.join(ARCHIVE_DIR, file)
        
    #     # Move the file
    #     shutil.move(src, dst)
    #     print(f"Moved {file} to archive.")

