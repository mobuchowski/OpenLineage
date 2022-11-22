from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("OpenLineage Spark Bigquery") \
    .getOrCreate()


PROJECT_ID = 'openlineage-ci'
DATASET_ID = 'airflow_integration'

source_table = f"{PROJECT_ID}.{DATASET_ID}.{spark.version}source"
target_table = f"{PROJECT_ID}.{DATASET_ID}.{spark.version}target"

spark.sparkContext.setLogLevel('info')

spark.sql(f"CREATE TABLE `{source_table}` IF NOT EXISTS  (value VARCHAR NOT NULL)")
spark.sql(f"INSERT INTO `{source_table}` VALUES ('test')")


first = spark.read.format('bigquery') \
    .option('table', source_table) \
    .load()


first.write.format('bigquery') \
    .option('table', target_table) \
    .option("temporaryGcsBucket", "spark-openlineage-tests") \
    .mode('append') \
    .save()
