gcloud dataproc jobs submit pyspark landing.py \
	--region=$DATAPROC_REGION \
	--cluster=$DATAPROC_CLUSTER_ID \
	--jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar \
	--driver-log-levels=root=INFO \
	--properties=spark.master=yarn
