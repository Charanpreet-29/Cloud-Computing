
#!/usr/bin/python
"""BigQuery I/O PySpark example."""
from pyspark.sql import SparkSession
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-project') \
  .getOrCreate()
bucket = spark.sparkContext._jsc.hadoopConfiguration().get(
    'fs.gs.system.bucket')
spark.conf.set('temporaryGcsBucket', bucket)
# Load data from BigQuery.
Repos = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data:github_repos.sample_repos') \
  .load()
Repos.createOrReplaceTempView('Repos')
# Perform word count.
watch_count = spark.sql('SELECT repo_name, watch_count FROM Repos ORDER BY watch_count DESC LIMIT 10')
watch_count.show()
watch_count.printSchema()
# Saving the data to BigQuery
watch_count.write.format('bigquery') \
  .option('table', 'project_dataset.watchcount_output') \
  .save()