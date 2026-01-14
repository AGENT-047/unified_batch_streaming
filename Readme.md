running guide

1. To run batch processing of dimension tables user_table and counteries_table

   a. first run command "docker compose up"
   b. In another shell run command "docker exec -it iceberg_pipeline bash"
   c. then run command "python3 /opt/scripts/main_script.py batch user"

   and if you want to run batch processing for countries_table
   c. run command "python3 /opt/scripts/main_script.py batch countries"

2. To run stream processing of events table

   first we need to run kafka stream producer code, which is going to provide
   random data to kafka, then start the kafka consumer code which is our
   spark structured streaming code which will write the data into warehouse in parquet format.

   a. if docker compose is not running already, then run it
   b. start producer for kafka: open shell run command "docker exec -it iceberg_pipeline bash"
   c. In the shell execute command "python3 /opt/scripts/kafka_data_producer.py"
   d. once producer is running now open another shell using command in step b.
   e. In the shell execute command
   "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/scripts/main_script.py streaming events"

3. To run main output_data, which is used for analytics

   a. if docker compose is not running already, then run it
   b. start producer for kafka: open shell run command "docker exec -it iceberg_pipeline bash"
   c. In the shell execute command "python3 /opt/scripts/output_data.py"
