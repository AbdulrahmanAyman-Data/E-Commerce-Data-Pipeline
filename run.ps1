# run.ps1
param(
    [Parameter(Mandatory=$true)]
    [string]$script,
    [switch]$local  # لو حطيت -local هيشتغل على Windows مباشرة
)

# انسخ آخر تعديلات للـ container
docker cp ".\src" spark-master-eco:/opt/spark-notebooks/
docker cp ".\config" spark-master-eco:/opt/spark-notebooks/

if ($local) {
    # شغّل على Windows مباشرة (للـ scripts اللي بتستخدم docker commands)
    $env:PYTHONPATH = "src"
    $env:ENV = "dev"
    python "src/$script"
} else {
    # شغّل جوّه Docker (للـ Spark jobs)
    docker exec -e PYTHONPATH=/opt/spark-notebooks/src -e ENV=dev spark-master-eco `
        /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --conf spark.hadoop.fs.defaultFS=hdfs://master:9000 `
        /opt/spark-notebooks/src/$script
}