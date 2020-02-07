#!/usr/bin/python
"""BigQuery I/O PySpark example."""
from pyspark.sql import SparkSession
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo2') \
  .getOrCreate()
# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector. This assumes the Cloud Storage connector for
# Hadoop is configured.
bucket = spark.sparkContext._jsc.hadoopConfiguration().get(
    'fs.gs.system.bucket')
spark.conf.set('temporaryGcsBucket', bucket)
# Load data from BigQuery.
langs = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data.github_repos.languages') \
  .load()
langs.createOrReplaceTempView('langs')
# language table simplification
repo_count = spark.sql('select repo_name,lang.name as LanguageName,lang.bytes as CodeBytes from(select repo_name,explode(language) as lang from langs as ls)')
repo_count.show()
repo_count.printSchema()
repo_count.createOrReplaceTempView('repo_count')
# Saving  the table
repo_count.write.format('bigquery') \
  .option('table', 'project_dataset.Simplyfied_Languages') \
  .save()
# PopulR REPOSITORY BASED ON MUBER OF BYTES
byte_count1 = spark.sql('SELECT repo_name,sum(CodeBytes) as TotalBytes FROM repo_count group by repo_name order by TotalBytes desc LIMIT 10')
byte_count1.show()
byte_count1.printSchema()
# Saving  the table
byte_count1.write.format('bigquery') \
  .option('table', 'project_dataset.byte_count1') \
  .save()