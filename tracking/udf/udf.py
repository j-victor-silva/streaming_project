import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


PATH = Path().resolve()

with open(
    PATH / "tracking/template/tracking_streaming_read_csv_template.json", "r"
) as template:
    schema_json = json.load(template)

type_mapping = {"StringType": StringType()}

schema = StructType(
    [
        StructField(field["name"], type_mapping[field["type"]], field["nullable"])
        for field in schema_json
    ]
)


def transform_csv_to_json():
    spark = (
        SparkSession.builder \
        .master("<SPARK_MASTER_URL>") \
        .config("spark.cores.max", "<NUMBER_OF_CORES>") \
        .appName("TrackingTransformData") \
        .getOrCreate()
    )

    temp_dir = str(PATH / "tracking/stream_read_temp/")
    read_stream_dir = str(PATH / "tracking/files/")
    output_stream_dir = str(PATH / "tracking/messages/")

    df = spark.readStream.csv(read_stream_dir, header=True, schema=schema, sep=";")
    df.createOrReplaceTempView("tracking")

    df_tratement = spark.sql(
        """
        SELECT
            to_date(dt_compra, 'dd/MM/yyyy') as purchase_date,
            ifnull(nullif(usuario, ""), "naoidentificado") as user_id,
            id_transacao as transaction_id,
            cast(valor_compra AS numeric) as revenue,
            cast(qtd_produtos AS numeric) as products_count,
            id_loja as store_id,
            nome_loja as store_name,
            id_vendedor as seller_id,
            nome_vendedor as seller_name
        FROM tracking
        WHERE
            id_transacao IS NOT NULL
    """
    )

    query = (
        df_tratement.writeStream.format("json") \
        .outputMode("append") \
        .option("checkpointlocation", temp_dir) \
        .option("path", output_stream_dir) \
        .trigger(processingTime="5 second") \
        .start() \
        .awaitTermination() \
    )


if __name__ == "__main__":
    transform_csv_to_json()
