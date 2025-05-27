from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("S3LogReader") \
    .config("spark.hadoop.fs.s3a.access.key", "<s3-access-key>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<s3-secret-key>") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read logs (text or CSV depending on your case)
df = spark.read.text("s3a://fintech-logs-prod/iPay/PayoutMiddleware/SmartSwift/2025-05-24/*.txt")

df.show(truncate=False)
print("TOTALLL", df.count())
