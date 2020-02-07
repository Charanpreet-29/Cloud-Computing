#!/usr/bin/python
"""BigQuery I/O PySpark example."""
from pyspark.sql import SparkSession
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo1') \
  .getOrCreate()
# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector. This assumes the Cloud Storage connector for
# Hadoop is configured.
bucket = spark.sparkContext._jsc.hadoopConfiguration().get(
    'fs.gs.system.bucket')
spark.conf.set('temporaryGcsBucket', bucket)
# Load data from BigQuery.
commits = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data.github_repos.sample_commits') \
  .load()
commits.createOrReplaceTempView('commits')
# popular repository by commit count for each year
commit_count = spark.sql('SELECT repo_name,COUNT(*) AS count FROM commits GROUP BY repo_name Order by COUNT(*) DESC  LIMIT 1000')
commit_count.show()
commit_count.printSchema()
# Saving the data to BigQuery
commit_count.write.format('bigquery') \
  .option('table', 'project_dataset.Output2') \
  .save()