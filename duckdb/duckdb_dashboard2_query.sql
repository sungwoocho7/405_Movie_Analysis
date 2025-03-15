WITH MovieData AS (
    SELECT 
        title AS movie_title,
        director,
        genres,
        vote_average AS avg_rating,
        revenue,
        budget,
        (revenue - budget) AS profit,
        release_date,
        EXTRACT(YEAR FROM release_date) AS release_year
    FROM read_csv_auto('/content/movies_cleaned.csv', HEADER=TRUE)
    WHERE revenue > 0  -- Exclude movies with zero revenue
)

-- Use a subquery to calculate window functions separately
SELECT 
    md.director,
    md.genres,
    md.release_year,

    -- Aggregate KPI Metrics
    COUNT(md.movie_title) AS movie_count,
    SUM(md.revenue) AS gross_revenue,
    AVG(md.revenue) AS avg_revenue,
    AVG(md.avg_rating) AS avg_movie_rating,
    SUM(md.profit) AS total_profit,

    revenue_trends.director_revenue_over_time,
    revenue_trends.director_rating_over_time

FROM MovieData md

-- Join with a subquery that handles window function
LEFT JOIN (
    SELECT 
        director,
        release_year,
        SUM(revenue) OVER(PARTITION BY director, release_year) AS director_revenue_over_time,
        AVG(avg_rating) OVER(PARTITION BY director, release_year) AS director_rating_over_time
    FROM MovieData
) AS revenue_trends
ON md.director = revenue_trends.director 
AND md.release_year = revenue_trends.release_year

WHERE 
    md.director IS NOT NULL 
    AND md.genres IS NOT NULL
    AND md.release_year BETWEEN 2000 AND 2023  -- Year Range Filter

GROUP BY md.director, md.genres, md.release_year, 
         revenue_trends.director_revenue_over_time, 
         revenue_trends.director_rating_over_time

ORDER BY md.director, md.release_year DESC;
