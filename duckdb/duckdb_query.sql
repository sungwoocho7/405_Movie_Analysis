WITH MovieData_CTE AS (
    SELECT 
        title,
        director,
        genres,
        vote_average,
        revenue,
        budget,
        release_date,
        EXTRACT(YEAR FROM release_date) AS release_year,
        EXTRACT(MONTH FROM release_date) AS release_month
    FROM read_csv_auto('/content/movies_cleaned (1).csv', HEADER=TRUE)
    WHERE revenue > 0  
)

SELECT 
    release_year,
    genres,
    COUNT(title) AS movie_count,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_movie_revenue,
    AVG(vote_average) AS avg_movie_rating
FROM MovieData_CTE
WHERE release_year IS NOT NULL AND genres IS NOT NULL
GROUP BY release_year, genres
ORDER BY release_year DESC;
