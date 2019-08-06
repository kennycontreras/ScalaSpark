
# create cluster
gcloud dataproc clusters create sod-cluster \
    --region us-central1 \
    --zone us-central1-a \
    --master-machine-type n1-standard-8 \
    --num-workers 40 \
    --num-master-local-ssds 2 \
    --worker-machine-type n1-standard-8 \
    --project $(gcloud config list project --format "value(core.project)") \
    --image-version 1.4.0-RC3-deb9

# execute job
"""
Args:
    CLUSTER_NAME = Name of the cluster

    1. Path of .avro files
    2. Bucket *** don't remove slash / ***
    3. Country
    4. Data Type (SLP, PLP)
    5. Flag 1=> Training Data, 2=> Validation Data
    6. Date YYYY-MM-DD format
"""

export CLUSTER_NAME = sod-cluster
gcloud dataproc jobs submit spark \
    --cluster ${CLUSTER_NAME} --region us-central1 \
    --properties spark.task.maxFailures=40,spark.jars.packages=org.apache.spark:spark-avro_2.11:2.4.0,spark.default.parallelism=640,spark.sql.shuffle.partitions=7690 \
    --class "SplitDataAvro" \
    --jars gs://bucket_testing_ml/project-spark/Split-Spark Project/spark_test_2.11-1.0.jar \
    -- "gs://bucket_testing_ml/pairwise/2019-06-30/CHL/SLP/*.avro" \
    "gs://bucket_testing_ml/" \
    "CHL" \
    "SLP" \
    "1" \
    "2019-06-30"
