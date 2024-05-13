from pyspark.sql import SparkSession
import os

# Set variables by environment
# Can be set in the .env file or argparser in the future
# TODO: Add logging support
# TODO: Add argparser
project_id = os.environ.get("TRUSTED_PROJECT_ID")
location = os.environ.get("TRUSTED_LOCATION")
trusted_bucket = os.environ.get("TRUSTED_BUCKET")
landing_bucket = os.environ.get("LANDING_BUCKET")

spark = (
    SparkSession.builder.appName("trusted-sentiment-analisis")
    .master("yarn")
    .getOrCreate()
)

landingDf = spark.read.parquet(f"gs://{landing_bucket}/sentiment-analisis")

transformedDf = landingDf.select(
    "data.id",
    "data.timestamp",
    "data.text",
    "data.sentiment.score",
    "data.sentiment.label",
    "data.product.id",
    "data.product.name",
    "data.user.id",
    "data.source",
)

trustedDf = transformedDf.write.parquet(f"gs://{trusted_bucket}/sentiment-analisis")
