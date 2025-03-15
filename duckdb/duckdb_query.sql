CREATE TABLE IF NOT EXISTS movies AS 
    SELECT * FROM read_csv_auto('/home/ubuntu/team19/data/movies_df.csv', HEADER=TRUE);

-- Ensure correct data types
ALTER TABLE movies ALTER revenue TYPE DOUBLE;
ALTER TABLE movies ALTER budget TYPE DOUBLE;
ALTER TABLE movies ALTER rating TYPE DOUBLE;

-- Ensure profit column exists
ALTER TABLE movies ADD COLUMN IF NOT EXISTS profit DOUBLE;
UPDATE movies SET profit = revenue - budget;

---------------------------
-- Dashboard 1: Executive Summary
---------------------------

-- KPI Summary
CREATE OR REPLACE VIEW measures_executive_summary AS (
    SELECT 
        EXTRACT(year FROM release_date) AS year,
        genres,
        COUNT(*) AS movie_count,
        SUM(revenue) AS total_revenue,
        AVG(revenue) AS avg_movie_revenue,
        SUM(budget) AS total_budget,
        AVG(budget) AS avg_budget,
        AVG(rating) AS avg_movie_rating
    FROM movies
    WHERE revenue > 0
    GROUP BY ROLLUP (year, genres)
);

-- Scatter Plot: Ratings vs. Revenue
CREATE OR REPLACE VIEW scatter_ratings_vs_revenue AS (
    SELECT rating, revenue
    FROM movies
    WHERE revenue > 0
);

-- Scatter Plot: Budget vs. Revenue
CREATE OR REPLACE VIEW scatter_budget_vs_revenue AS (
    SELECT budget, revenue
    FROM movies
    WHERE budget > 0 AND revenue > 0
);

-- Monthly Revenue Trends (Fixed Name)
CREATE OR REPLACE VIEW line_monthly_revenue AS (
    SELECT 
        EXTRACT(year FROM release_date) AS year,
        EXTRACT(month FROM release_date) AS month,
        SUM(revenue) AS total_revenue
    FROM movies
    WHERE revenue > 0
    GROUP BY year, month
    ORDER BY year, month
);

-- Revenue by Director
CREATE OR REPLACE VIEW bar_revenue_by_director AS (
    SELECT director, SUM(revenue) AS total_revenue
    FROM movies
    WHERE director IS NOT NULL AND revenue > 0
    GROUP BY director
    ORDER BY total_revenue DESC
    LIMIT 20  -- Top 20
);

-- Revenue by Genre
CREATE OR REPLACE VIEW bar_revenue_by_genre AS (
    SELECT genres, SUM(revenue) AS total_revenue
    FROM movies
    WHERE genres IS NOT NULL AND revenue > 0
    GROUP BY genres
    ORDER BY total_revenue DESC
);

---------------------------
-- Dashboard 2: Movie Data Analysis on Director
---------------------------

-- Director’s Performance Over Time
CREATE OR REPLACE VIEW measures_by_director AS (
    SELECT 
        director,
        genres,
        EXTRACT(year FROM release_date) AS release_year,
        COUNT(*) AS movie_count,
        SUM(revenue) AS gross_revenue,
        AVG(revenue) AS avg_revenue,
        AVG(rating) AS avg_movie_rating,
        SUM(profit) AS total_profit
    FROM movies
    WHERE revenue > 0 AND director IS NOT NULL
    GROUP BY director, genres, release_year
);

-- Director’s Revenue & Ratings Over Time
CREATE OR REPLACE VIEW trend_director_revenue_ratings AS (
    SELECT 
        director,
        EXTRACT(year FROM release_date) AS release_year,
        AVG(rating) AS avg_movie_rating,
        SUM(revenue) AS total_revenue
    FROM movies
    WHERE director IS NOT NULL
    GROUP BY director, release_year
    ORDER BY director, release_year
);

-- Highest-Grossing Movies (Revenue & Rating)
CREATE OR REPLACE VIEW bar_top_movies AS (
    SELECT 
        title AS movie_title,
        director,
        revenue AS movie_revenue,
        rating AS movie_rating
    FROM movies
    WHERE revenue > 0
    ORDER BY revenue DESC
    LIMIT 20
);

