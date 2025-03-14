# Import and setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    trunc,
    udf,
    when,
    col,
    trim,
    count,
    to_date,
    regexp_replace
)
from pyspark.sql.types import StringType
import pandas as pd
import unicodedata
import re
# Initialize Spark
spark = SparkSession.builder \
    .getOrCreate()

sc = spark.sparkContext




# Load the CSV file (force dtype=str to prevent data type auto-conversion)
file_path = "/home/ubuntu/team19/data/IMBD.csv" 
df = pd.read_csv(file_path, dtype=str)

# Function to remove all quotes in description
def clean_description(text):
    if pd.isna(text):
        return text  # Keep NaN values as they are
    return text.replace('"', '')  # Remove all quotes

# Apply only to the 'description' column
if "description" in df.columns:
    df["description"] = df["description"].apply(clean_description)

if "stars" in df.columns:
    df["stars"] = df["stars"].apply(clean_description)

# Save the cleaned CSV
cleaned_file_path = "/home/ubuntu/team19/data/IMBD_cleaned.csv"
df.to_csv(cleaned_file_path, index=False)




# reading the CSV file
base_path = '/home/ubuntu/team19/data/'

cpi_file = base_path + 'CPI_data.csv'
tmdb_file = base_path + 'TMDB_movie_dataset_v11.csv'
imbd_file = base_path + 'IMBD_cleaned.csv'

# Load CPI data using Pandas
cpi = spark.read.csv(cpi_file, header=True, inferSchema=True)

# Load TMDB and IMDB data using Spark
df_tmdb = spark.read.csv(tmdb_file, header=True, inferSchema=True)
df_imbd = spark.read.csv(imbd_file, header=True, inferSchema=True)



# Convert TMDB 'vote_average' and 'vote_count' to numerical values
df_tmdb = df_tmdb.withColumn("vote_average", col("vote_average").cast("float")) \
                 .withColumn("vote_count", col("vote_count").cast("int"))

# Convert TMDB 'release_date' to date
df_tmdb = df_tmdb.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

# Convert TMDB 'revenue', 'runtime', and 'budget' to numerical values
df_tmdb = df_tmdb.withColumn("revenue", col("revenue").cast("int")) \
                 .withColumn("runtime", col("runtime").cast("int")) \
                 .withColumn("budget", col("budget").cast("int"))

# Convert CPI Data 'observation_date' to date
cpi = cpi.withColumn("observation_date", to_date(col("observation_date"), "yyyy-MM-dd"))

# Convert IMBD rating to Float
df_imbd = df_imbd.withColumn("rating", col("rating").cast("float"))

# Convert IMBD runtime to numerical values
df_imbd = df_imbd.withColumn("runtime", regexp_replace(col("runtime"), "[^0-9]", ""))
df_imbd = df_imbd.withColumn("runtime", col("runtime").cast("int"))



# Convert release_date to the first day of the month
df_tmdb = df_tmdb.withColumn("release_month", trunc(col("release_date"), "MONTH"))


# Function to fix encoding issues
def fix_encoding(text):
    if text is None or text.strip() == "" or text in ["[]", "nan", "NaN", "null", "NULL"]:  # Handle missing values
        return None
    try:
        # Normalize Unicode encoding (fix special characters)
        text = text.encode("latin1").decode("utf-8")  # Fix possible encoding issues
        text = unicodedata.normalize("NFKC", text)  # Normalize special characters
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass  # If decoding fails, keep the original text

    text = text.replace("[", "").replace("]", "").replace("'", "").strip()

    return text if text else None  # Remove unnecessary whitespace

# Convert function into PySpark UDF
fix_encoding_udf = udf(fix_encoding, StringType())

# Apply UDF to fix encoding issues in relevant columns
for col_name in ["movie", "stars", "genre"]:
    df_imbd = df_imbd.withColumn(col_name, fix_encoding_udf(df_imbd[col_name]))

for col_name in ["title"]:
    df_tmdb = df_tmdb.withColumn(col_name, fix_encoding_udf(df_tmdb[col_name]))

# Function to fix encoding issues
def fix_encoding_v2(text):
    if text is None or text.strip() == "" or text in ["[]", "nan", "NaN", "null", "NULL"]:  # Handle missing values
        return None
    try:
        # Normalize Unicode encoding (fix special characters)
        text = text.encode("latin1").decode("utf-8")  # Fix possible encoding issues
        text = unicodedata.normalize("NFKC", text)  # Normalize special characters
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass  # If decoding fails, keep the original text

    # Detect unexpected characters and remove them dynamically
    text = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ'. ]", "", text)  # Keeps only valid letters, accents, apostrophes, periods

    text = text.replace("[", "").replace("]", "").replace("'", "").strip()

    return text if text else None  # Remove unnecessary whitespace

# Convert function into PySpark UDF
fix_encoding_v2_udf = udf(fix_encoding_v2, StringType())

# Apply the function to the "director" column
df_imbd = df_imbd.withColumn("director", fix_encoding_v2_udf("director"))

# Define mapping for certification categories
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



# Drop rows with missing essential values
df_tmdb = df_tmdb.na.drop(subset=["title"])

# Drop duplicate rows based on title + release year
df_tmdb = df_tmdb.dropDuplicates(["title", "runtime"])

# Drop rows with missing essential values
df_imbd = df_imbd.withColumn("director", when(trim(col("director")) == "", None).otherwise(col("director")))
df_imbd = df_imbd.na.drop(subset=["director"])

# Drop duplicate rows based on title + release year
df_imbd = df_imbd.dropDuplicates(["movie", "runtime"])

df_imbd = df_imbd.withColumnRenamed("runtime", "runtime_")

from pyspark.sql.functions import explode, split

# Assuming genres are stored as a comma-separated string, first split them into an array
df_imbd = df_imbd.withColumn("genre_array", split(col("genre"), ", "))

# Use explode() to create a new row for each genre
df_imbd = df_imbd.withColumn("genre_new", explode(col("genre_array")))

# Drop the original genre column (optional)
df_imbd = df_imbd.drop("genre", "genre_array")


# prompt: for tmdb, delete the ones who's runtime is 0

# Filter out rows where runtime is 0
df_tmdb = df_tmdb.filter(df_tmdb["runtime"] > 0)

df_tmdb = df_tmdb.filter((col("title").isNotNull()) & (col("title") != ""))

df_tmdb = df_tmdb.filter(col("status") == "Released")



from pyspark.sql.functions import lower

# Convert title columns to lowercase for case-insensitive comparison
df_tmdb = df_tmdb.withColumn("title_lower", lower(col("title")))
df_imbd = df_imbd.withColumn("movie_lower", lower(col("movie")))

# Perform inner join based on lowercase titles and runtime
joined_df = df_tmdb.join(df_imbd, df_tmdb["title_lower"] == df_imbd["movie_lower"] , "inner").filter(df_tmdb["runtime"] == df_imbd["runtime_"])

# Perform inner join on joined_df and cpi_data based on movie release dates
final_joined_df = joined_df.join(cpi, joined_df["release_month"] == cpi["observation_date"], "inner")


# Retreive CPI value for 2025-02-01 and Set it as the Base Value
cpi_base_value = cpi.filter(col("observation_date") == "2025-02-01").select("CPIAUCSL").collect()[0][0]

from pyspark.sql.functions import col

# Create Inflation-Adjusted Revenue and Budget Columns
final_joined_df = final_joined_df.withColumn(
    "budget_adjusted_cpi", col("budget") * (cpi_base_value / col("CPIAUCSL"))
).withColumn(
    "revenue_adjusted_cpi", col("revenue") * (cpi_base_value / col("CPIAUCSL")))

final_joined_df_filtered = final_joined_df.filter(col("revenue") > 0)

columns_to_drop = [
    "id", "vote_count", "backdrop_path", "homepage", "imdb_id",
    "original_language", "original_title", "overview", "popularity",
    "poster_path", "tagline", "production_companies", "production_countries",
    "spoken_languages", "stars", "keywords", "description", "votes", "vote_average"
]

movies_df = final_joined_df_filtered.drop(*columns_to_drop)



# Save as a single CSV file under a temp directory
output_path = "/home/ubuntu/team19/data/movies_cleaned_csv"

movies_df.coalesce(1)\
         .write\
         .mode("overwrite")\
         .option("header", "true")\
         .csv(output_path)

print(f"CSV file saved to: {output_path}")
