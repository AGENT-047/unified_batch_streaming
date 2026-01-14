# Start from the image you were already using
FROM tabulario/spark-iceberg:3.5.0_1.4.2

# Switch to root user to install packages (permission safety)
USER root

# Install the Python Kafka library
RUN pip install kafka-python

# (Optional) Switch back to the default user if the base image uses one
# usually 'spark' or 'jovyan', but for this specific image root is fine/standard.
