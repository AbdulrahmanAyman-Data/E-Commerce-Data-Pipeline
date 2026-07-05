# run.ps1
param(
    [Parameter(Mandatory=$true)]
    [string]$script
)

# انسخ آخر تعديلات للـ container
docker cp ".\src" spark-master-eco:/opt/spark-notebooks/
docker cp ".\config" spark-master-eco:/opt/spark-notebooks/

# شغّل الـ script
docker exec -e PYTHONPATH=/opt/spark-notebooks/src -e ENV=dev spark-master-eco `
    /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.hadoop.fs.defaultFS=hdfs://master:9000 `
    /opt/spark-notebooks/src/$script