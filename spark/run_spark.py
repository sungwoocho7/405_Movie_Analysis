import os
import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, count, explode, split, to_date,
    regexp_replace, when, udf
)
from pyspark.sql.types import StringType
'''
# ========== STEP 1: SETUP SPARK ==========
spark = SparkSession.builder.getOrCreate()

# Get the absolute path to your data folder
HOME_DIR = os.path.expanduser("~")
DATA_PATH = os.path.join(HOME_DIR, "team19", "data")

# ========== STEP 2: LOAD DATA ==========
df_tmdb = spark.read.csv(f"{DATA_PATH}/TMDB_movie_dataset_v11.csv", header=True, inferSchema=True)
df_imbd = spark.read.csv(f"{DATA_PATH}/IMBD.csv", header=True, inferSchema=True)

# ========== STEP 3: CLEAN TMDB DATA ==========
df_tmdb = df_tmdb.withColumn("vote_average", col("vote_average").cast("float")) \
                 .withColumn("vote_count", col("vote_count").cast("int")) \
                 .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd")) \
                 .withColumn("revenue", col("revenue").cast("int")) \
                 .withColumn("runtime", col("runtime").cast("int")) \
                 .withColumn("budget", col("budget").cast("int"))

df_tmdb = df_tmdb.na.drop(subset=["title"])
df_tmdb = df_tmdb.dropDuplicates(["title", "runtime"])
df_tmdb = df_tmdb.filter(df_tmdb["runtime"] > 0)
df_tmdb = df_tmdb.filter(col("status") == "Released")

# ========== STEP 4: CLEAN IMBD DATA ==========
df_imbd = df_imbd.withColumn("rating", col("rating").cast("float")) \
                 .withColumn("runtime", regexp_replace(col("runtime"), "[^0-9]", "").cast("int"))

df_imbd = df_imbd.withColumn("director", when(trim(col("director")) == "", None).otherwise(col("director")))
df_imbd = df_imbd.na.drop(subset=["director"])
df_imbd = df_imbd.dropDuplicates(["movie", "runtime"])

# ========== STEP 5: FIX ENCODING ISSUES ==========
def fix_encoding(text):
    if text is None or text.strip() in ["", "[]", "nan", "NaN", "null", "NULL"]:
        return None
    try:
        text = text.encode("latin1").decode("utf-8")
        text = unicodedata.normalize("NFKC", text)
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    return text.replace("[", "").replace("]", "").replace("'", "").strip() or None

fix_encoding_udf = udf(fix_encoding, StringType())

for col_name in ["director", "movie", "stars", "genre"]:
    df_imbd = df_imbd.withColumn(col_name, fix_encoding_udf(df_imbd[col_name]))

# ========== STEP 6: STANDARDIZE CERTIFICATION RATINGS ==========
df_imbd = df_imbd.withColumn(
    "certificate",
    when(col("certificate").isin(["G", "U"]), "G")
    .when(col("certificate").isin(["PG", "M/PG", "TV-PG"]), "PG")
    .when(col("certificate").isin(["PG-13", "UA 13+", "13+", "UA 7+", "U/A 7+", "TV-14"]), "PG-13")
    .when(col("certificate").isin(["R", "M"]), "R")
    .when(col("certificate").isin(["NC-17", "X"]), "NC-17")
    .when(col("certificate").isin(["18+", "18", "16+", "16"]), "18+")
    .when(col("certificate").isin(["15+", "15"]), "15+")
    .when(col("certificate").isin(["12+", "12", "UA 13+", "13"]), "12+")
    .when(col("certificate").isin(["Not Rated", "Unrated", "Passed", "Approved", "All"]), "Not Rated")
    .when(col("certificate") == "(Banned)", "Banned")
    .otherwise(col("certificate"))
)

# ========== STEP 7: MATCH MOVIES FROM BOTH DATASETS ==========
df_tmdb = df_tmdb.withColumn("title_lower", lower(col("title")))
df_imbd = df_imbd.withColumn("movie_lower", lower(col("movie")))

joined_df = df_tmdb.join(df_imbd, 
                         (df_tmdb["title_lower"] == df_imbd["movie_lower"]) & 
                         (df_tmdb["runtime"] == df_imbd["runtime"]), 
                         "inner")

# Save merged data
joined_df.write.mode("overwrite").parquet(f"{DATA_PATH}/merged_movies.parquet")
'''

print(f"Data cleaned and merged! {joined_df.count()} rows saved as merged_movies.parquet.")



'''
import os
from pyspark.sql import SparkSession

# Ensure Java is set up correctly
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# Initialize Spark
spark = SparkSession.builder \
    .appName('ec2-spark') \
    .config('spark.driver.memory', '4g') \  # Adjust based on your instance size
    .config('spark.sql.warehouse.dir', '/home/ubuntu/spark-warehouse') \
    .config('spark.checkpoint.dir', '/home/ubuntu/spark-checkpoint') \
    .getOrCreate()

sc = spark.sparkContext
'''


