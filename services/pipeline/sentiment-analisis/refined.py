from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max
import os

# Set variables by environment
# Can be set in the .env file or argparser in the future
# TODO: Add logging support
# TODO: Add argparser
project_id = os.environ.get("REFINED_PROJECT_ID")
location = os.environ.get("REFINED_LOCATION")
trusted_marketplace_bucket = os.environ.get("TRUSTED_MARKETPLACE_BUCKET")
trusted_pricing_bucket = os.environ.get("TRUSTED_PRICING_BUCKET")
trusted_sentiment_analysis_bucket = os.environ.get("TRUSTED_PRODUCT_SENTIMENT_BUCKET")
refined_product_sentiment_bucket = os.environ.get("REFINED_PRODUCT_SENTIMENT_BUCKET")

spark = SparkSession.builder.appName("product-sentiment").master("yarn").getOrCreate()

trustedMarketplaceDf = spark.read.parquet(
    f"gs://{trusted_marketplace_bucket}/marketplace"
)
trustedPricingDf = spark.read.parquet(f"gs://{trusted_pricing_bucket}/pricing")
trustedSentimentDf = spark.read.parquet(
    f"gs://{trusted_sentiment_analysis_bucket}/sentiment-analisis"
)


# Business Case: How sentiment can leverage the product price?
transactionsMktplaceDf = trustedMarketplaceDf.sql(
    "select * where order_date between '2024-01-01' and '2024-04-01'"
)

meanPricesDf = trustedPricingDf.sql(
    "select product_id, avg(average_price) as average_price, avg(minimum_price) as minimum_price, avg(maximium_price) as maximum_price where survey_date between '2024-01-01' and '2024-04-01' group by product_id"
)

meanSentimentDf = trustedSentimentDf.sql(
    "select product_id, sentiment_label, avg(sentiment_score) as sentiment_score where timestamp between '2024-01-01' and '2024-04-01' group by product_id, sentiment_label"
)

refinedDf = transactionsMktplaceDf.join(meanPricesDf, "product_id").join(
    meanSentimentDf, "product_id"
)

refinedDf = refinedDf.select(
    "transaction_id",
    "order_date",
    "value",
    "product_id",
    "product_name",
    "sentiment_score",
    "sentiment_label",
    "average_price",
    "minimum_price",
    "maximum_price",
)

refinedDf.write.parquet(f"gs://{refined_product_sentiment_bucket}/product-sentiment")
