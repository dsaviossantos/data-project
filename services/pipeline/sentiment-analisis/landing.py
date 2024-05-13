from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import to_json
import os


# Set variables by environment
# Can be set in the .env file or argparser in the future
# TODO: Add logging support
# TODO: Add argparser
project_id = os.environ.get("PUBSUBLITE_PROJECT_ID")
location = os.environ.get("PUBSUBLITE_LOCATION")
subscription_id = os.environ.get("PUBSUBLITE_SUBSCRIPTION_ID")
landing_bucket = os.environ.get("LANDING_BUCKET")


spark = (
    SparkSession.builder.appName("landing-sentiment-analisis")
    .master("yarn")
    .getOrCreate()
)

subscribedDf = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        f"projects/{project_id}/locations/{location}/subscriptions/{subscription_id}",
    )
    .load()
)

subscribedDf = subscribedDf.withColumn(
    "data",
    subscribedDf.data.cast(StringType()),
)
subscribedDf = subscribedDf.withColumn(
    "attributes",
    to_json(subscribedDf["attributes"]),
)

stream = (
    subscribedDf.writeStream.format("parquet")
    .option("path", f"gs://{landing_bucket}/sentiment-analisis")
    .start()
)

stream.awaitTermination(120)
stream.stop()
