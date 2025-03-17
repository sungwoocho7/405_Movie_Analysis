# Group 19 MSBA 405 Pipeline (Final Project)
# Pipeline to Understanding Movie Revenue, Ratings, and Industry Trends 

## Data Description

### Data Source
For this analysis, we used a total of three datasets:

We went to this page https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies/data and downloaded the Movies data from 2008 - 2023 in csv format. 

We also downloaded genre, lead actors, and movie directors information from: 

https://www.kaggle.com/datasets/gayu14/tv-and-movie-metadata-with-genres-and-ratings-imbd 

We then downloaded the inflation rate data from the Federal Reserve Economic Database: 

https://fred.stlouisfed.org/graph/?g=rocU 

### Data Download
In order to download the data from kaggle, you need to set up Kaggle API

1. **Go to [Kaggle](https://www.kaggle.com/account) → API → Create New API Token**  
2. **Download `kaggle.json` and replace the placeholder in `team19/.kaggle/`**  
   ```sh
   mv kaggle.json ~/team19/.kaggle/
   chmod 600 ~/team19/.kaggle/kaggle.json
   ```
3. **The bash pipeline `run_pipeline.sh` will automatically download the dataset into the data pipeline**

### Pipeline

1. **Extraction**
   Kaggle data is downloaded using API json key, CPI data is downloaded with `wget`
   All data files are saved in `team19/data`

2. **Transform**
   Data is pulled from data folder and fed into pyspark for cleaning.
   Processed data is saved back into `team19/data`

3. **Lead**
   DuckDB database intialized under `team19/duckdb`
   DuckDB executes SQL to create views
   

### Tableau Visualization
We created and publcished an interactive tableau dashboard to visualize our data analysis: 

https://public.tableau.com/views/405FinalProject_YS/OverviewMovieRevenueAnalysis?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link 


