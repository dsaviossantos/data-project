gcloud dataproc jobs submit pyspark refined.py \
	--region=$DATAPROC_REGION \
	--cluster=$DATAPROC_CLUSTER_ID \
	--driver-log-levels=root=INFO \
	--properties=spark.master=yarn
