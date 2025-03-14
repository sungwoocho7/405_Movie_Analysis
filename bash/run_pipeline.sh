#!/bin/bash

# ========== CONFIGURATION ==========
KAGGLE_JSON_PATH=~/team19/.kaggle/kaggle.json
DATASET_PATH=~/team19/data
SPARK_SCRIPT=~/team19/spark/run_spark.py

# List of datasets (Format: "dataset-name|kaggle-slug")
DATASETS=(
    "tmdb-movies|asaniczka/tmdb-movies-dataset-2023-930k-movies"
    "tv-movie-metadata|gayu14/tv-and-movie-metadata-with-genres-and-ratings-imbd"
)

# ========== STEP 1: CHECK KAGGLE API SETUP ==========
echo "Checking Kaggle API setup..."
if [ ! -f "$KAGGLE_JSON_PATH" ]; then
    echo "ERROR: kaggle.json not found at $KAGGLE_JSON_PATH"
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Installing jq..."
    sudo apt-get update && sudo apt-get install -y jq
fi

# Create data directory if it doesn’t exist
mkdir -p $DATASET_PATH

# ========== STEP 2: DOWNLOAD DATASETS FROM KAGGLE ==========
echo "Downloading datasets from Kaggle..."
for DATA in "${DATASETS[@]}"; do
    DATASET_NAME=$(echo $DATA | cut -d "|" -f 1)
    KAGGLE_SLUG=$(echo $DATA | cut -d "|" -f 2)

    echo "Downloading dataset: $DATASET_NAME..."

    kaggle datasets download $KAGGLE_SLUG -p $DATASET_PATH --unzip

    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to download $DATASET_NAME!"
        continue
    fi

done

echo "All Kaggle datasets downloaded and extracted successfully."

# ========== STEP 3: DOWNLOAD CPI DATA ==========
echo "Downloading CPI data from FRED..."

CPI_URL="https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL"
CPI_FILE="$DATASET_PATH/CPI_data.csv"

wget --no-check-certificate --user-agent="Mozilla/5.0" "$CPI_URL" -O "$CPI_FILE"

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to download CPI data!"
else
    echo "CPI data downloaded successfully: $CPI_FILE"
fi

# ========== STEP 4: EXECUTE SPARK SCRIPT ==========
echo "Running Spark script..."
spark-submit $SPARK_SCRIPT

if [ $? -ne 0 ]; then
    echo "ERROR: Spark script execution failed!"
    exit 1
fi

# ========== STEP 5: RENAME SPARK CSV OUTPUT ==========
echo "Renaming Spark-generated CSV..."

cd $DATASET_PATH/movies_cleaned_csv/
mv part-*.csv ../movies_df.csv

cd ..
rm -r movies_cleaned_csv

echo "CSV successfully renamed to movies_df.csv"
