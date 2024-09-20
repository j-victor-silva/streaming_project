import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


PATH = Path().resolve()

with open(
    PATH / "profiles/template/profiles_streaming_read_csv_template.json", "r"
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
        .appName("ProfilesTransformData") \
        .getOrCreate()
    )

    temp_dir = str(PATH / "profiles/stream_read_temp/")
    read_stream_dir = str(PATH / "profiles/files/")
    output_stream_dir = str(PATH / "profiles/messages/")

    df = spark.readStream.csv(read_stream_dir, header=True, schema=schema, sep=";")
    df.createOrReplaceTempView("profiles")

    df_tratement = spark.sql(
        """
        SELECT
            usuario as user_id,
            nome as name,
            telefone as phone,
            email,
            endereco as adress,
            cidade as city,
            estado as state,
            genero as gender,
            to_date(dt_aniversario, 'dd/MM/yyyy') as birthday,
            to_timestamp(to_date(dt_cadastro, 'dd/MM/yyyy')) as created_at
        FROM profiles
        WHERE
            usuario IS NOT NULL
    """
    )

    query = (
        df_tratement.writeStream.format("json") \
        .outputMode("append") \
        .option("checkpointlocation", temp_dir) \
        .option("path", output_stream_dir) \
        .trigger(processingTime="5 second") \
        .start() \
        .awaitTermination()
    )


if __name__ == "__main__":
    transform_csv_to_json()
