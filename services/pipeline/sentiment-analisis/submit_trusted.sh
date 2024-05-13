gcloud dataproc jobs submit pyspark trusted.py \
	--region=$DATAPROC_REGION \
	--cluster=$DATAPROC_CLUSTER_ID \
	--driver-log-levels=root=INFO \
	--properties=spark.master=yarn
