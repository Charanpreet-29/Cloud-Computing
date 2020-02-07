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
sample_commits = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data.github_repos.sample_commits') \
  .load()
sample_commits.createOrReplaceTempView('sample_commits')
# commit table simplification
repocommit_count = spark.sql('select isoyear as year, count(RepoName) as Total_Repos, sum(repo_count) as TotalCommits from (SELECT COUNT(repo_name) AS repo_count, YEAR(committer.date) AS isoyear,$
repocommit_count.show()
repocommit_count.printSchema()
# Saving  the table
repocommit_count.write.format('bigquery') \
  .option('table', 'project_dataset.Year_repo_commit_count') \
  .save()