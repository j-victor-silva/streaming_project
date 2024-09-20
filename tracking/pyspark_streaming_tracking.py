import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, current_timestamp, encode, base64, date_format
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    IntegerType,
    FloatType,
)


with open(
    "tracking/template/tracking_streaming_write_to_postgresql_template.json", "r"
) as template:
    schema_json = json.load(template)

type_mapping = {
    "StringType": StringType(),
    "DateType": DateType(),
    "FloatType": FloatType(),
    "IntegerType": IntegerType(),
}

schema = StructType(
    [
        StructField(field["name"], type_mapping[field["type"]], field["nullable"])
        for field in schema_json
    ]
)


def run_streaming():
    PATH = Path().resolve()
    spark = (
        SparkSession.builder \
        .appName("TrackingStreaming") \
        .master("<SPARK_MASTER_URL>") \
        .config("spark.cores.max", "<NUMBER_OF_CORES>") \
        .getOrCreate()
    )

    temp_dir = str(PATH / "tracking/stream_write_temp/")
    write_stream_dir = str(PATH / "tracking/messages/")

    df = spark.readStream.json(write_stream_dir, schema=schema)
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    message_id = base64(
        encode(
            concat(
                df["transaction_id"],
                df["user_id"],
                date_format(current_timestamp(), "yyyy-MM-dd HH-mm-ss"),
            ),
            "UTF-8",
        )
    )

    df = df.withColumn("message_id", message_id)

    def insert_postgres(dataframe, batchId):
        dataframe.write.format("jdbc") \
        .option("url", "<POSTGRESQL_URL>") \
        .option("dbtable", "<POSTGRESQL_TABLE>") \
        .option("user", "<POSTGRESQL_USER>") \
        .option("password", "<POSTGRESQL_PASSWORD>") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    query = (
        df.writeStream.foreachBatch(insert_postgres) \
        .outputMode("append") \
        .trigger(processingTime="5 second") \
        .option("checkpointlocation", temp_dir) \
        .start() \
        .awaitTermination()
    )


if __name__ == "__main__":
    run_streaming()
