# ScalaSpark
Demo to Deploy a Job on Dataproc for GCP  

### Command example

```bash
gcloud dataproc jobs submit spark \
    --cluster test-spark --region us-central1 \
    --class "SparkTest" \
    --jars jarpath \
    -- firstargument \
    -- secondargument
```
