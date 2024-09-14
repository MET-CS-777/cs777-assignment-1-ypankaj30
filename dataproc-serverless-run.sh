### Create Dataproc persistent history cluster (PHS)

# Default infra resources:
gcloud dataproc clusters create cs777-assignment-1-phs --project met-cs-777-434703 --region us-east1 --single-node --enable-component-gateway --properties spark:spark.history.fs.logDirectory=gs://met-cs777-assignment1/phs/*/spark-job-history

# Specific infra resources:
gcloud dataproc clusters create cs777-assignment-1-phs --project met-cs-777-434703 --region us-east1 --zone us-east1-b --single-node --master-machine-type n4-standard-2 --master-boot-disk-size 500 --image-version 2.2-debian12 --enable-component-gateway --properties "yarn:yarn.nodemanager.remote-app-log-dir=gs://met-cs777-assignment1/*/,spark:spark.history.fs.logDirectory=gs://met-cs777-assignment1/*/spark-job-historyyarn-logs,spark:spark.eventLog.dir=gs://met-cs777-assignment1/events/spark-job-history"

# Delete PHS cluster
gcloud dataproc clusters delete cs777-assignment-1-phs --project met-cs-777-434703 --region us-east1

# Submit PySpark job to Dataproc serverless WITHOUT PHS - WORKS
gcloud dataproc batches submit --project met-cs-777-434703 --region us-east1 pyspark gs://met-cs777-assignment1/Yawale_Pankaj_Assignment_1.py

# Submit PySpark job to Dataproc serverless WITH PHS
gcloud dataproc batches submit --project met-cs-777-434703 --region us-east1 pyspark gs://met-cs777-assignment1/Yawale_Pankaj_Assignment_1.py --history-server-cluster=projects/met-cs-777-434703/regions/us-east1/clusters/cs777-assignment-1-phs --properties spark.executor.instances=5,spark.driver.cores=4,spark.executor.cores=4,spark.app.name=Yawale_Pankaj_Assignment_1

# DELETE serverless batch - Change  batchID

gcloud dataproc batches cancel 36e754a79e224d3286c8dbc941d74153 --project met-cs-777-434703 --region us-east1

# Test smaller dataset WITH PHS
gcloud dataproc batches submit --project met-cs-777-434703 --region us-east1 pyspark gs://met-cs777-assignment1/test.py --history-server-cluster=projects/met-cs-777-434703/regions/us-east1/clusters/cs777-assignment-1-phs
