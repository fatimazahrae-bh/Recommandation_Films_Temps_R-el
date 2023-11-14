import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

# Configure Spark
SparkSession.builder.config(conf=SparkConf())
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .getOrCreate()

# Define Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movie_ratings") \
    .option("startingOffsets", "earliest") \
    .load()

# Define JSON schema
json_schema = StructType([
    StructField("adult", BooleanType(), True),
    StructField("belongs_to_collection", StructType([
        StructField("name", StringType(), True),
        StructField("poster_path", StringType(), True),
        StructField("backdrop_path", StringType(), True)
    ]), True),
    StructField("budget", IntegerType(), True),
    StructField("genres", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("original_language", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("production_companies", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("logo_path", StringType(), True),
        StructField("name", StringType(), True),
        StructField("origin_country", StringType(), True)
    ])), True),
    StructField("production_countries", ArrayType(StructType([
        StructField("iso_3166_1", StringType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("title", StringType(), True),
    StructField("video", BooleanType(), True),
    StructField("vote_average", StringType(), True),
    StructField("vote_count", IntegerType(), True)
])

# Extract and transform data
value_df = df.selectExpr("CAST(value AS STRING)")
parsed_df = value_df.select(F.from_json(value_df["value"], json_schema).alias("values")).select("values.*")

# Select relevant columns
result_df = parsed_df.select(
    "overview",
    F.col("production_companies.name").alias("name_production_company"),
    F.col("production_companies.origin_country").alias("origin_country_company"),
    "release_date",
    "status",
    "original_language",
    "tagline",
    "title",
    "video",
    "vote_average",
    "vote_count"
)

# Define Elasticsearch sink
query = result_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "movies") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "true") \
    .option("es.index.auto.create", "true") \
    .option("checkpointLocation", "./checkpointLocation/") \
    .start()

# Start the console sink for debugging
query = result_df.writeStream.outputMode("append").format("console").start()

# Wait for the termination of the streaming query
query.awaitTermination()
