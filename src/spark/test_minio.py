from session_builder import get_spark_session

def test_delta_write():
    print("Initializing Spark Session...")
    spark = get_spark_session("MinIOTest")

    data = [
        {"id": 1, "status": "success", "user": "admin"},
        {"id": 2, "status": "failed", "user": "guest"}
    ]
    df = spark.createDataFrame(data)

    print("Writing dummy data to MinIO as Delta table...")
    test_path = "s3a://siem-lakehouse/test_delta"
    
    df.write.format("delta").mode("overwrite").save(test_path)
    print(f"Successfully wrote to {test_path}")

    print("Reading data back from MinIO...")
    read_df = spark.read.format("delta").load(test_path)
    read_df.show()

    spark.stop()

if __name__ == "__main__":
    test_delta_write()