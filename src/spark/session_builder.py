import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

load_dotenv()

# Inject JVM argument to fix Java 21+ Hadoop compatibility
os.environ["_JAVA_OPTIONS"] = "-Djava.security.manager=allow"

def get_spark_session(app_name="SIEM_Lakehouse"):
    # Define the required JAR packages for Kafka and MinIO (S3)
    extra_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    minio_user = os.getenv("MINIO_USER")
    minio_password = os.getenv("MINIO_PASSWORD")

    builder = SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Inject the Delta Lake dependencies
    return configure_spark_with_delta_pip(builder, extra_packages).getOrCreate()

if __name__ == "__main__":
    print("Building Spark Session and downloading JARs (this may take a minute on first run)...")
    spark = get_spark_session()
    print("Spark Session initialized successfully!")
    spark.stop()