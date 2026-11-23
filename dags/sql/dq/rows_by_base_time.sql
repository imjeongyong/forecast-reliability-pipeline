WITH per_file AS (
    SELECT base_date, base_time, nx, ny, loaded_rows
    FROM raw.load_log
    WHERE status = 'loaded'
),
usual AS (
    SELECT base_time,
           mode() WITHIN GROUP (ORDER BY loaded_rows) AS usual_rows
    FROM per_file
    GROUP BY base_time
)
SELECT f.base_date, f.base_time, f.nx, f.ny, f.loaded_rows, u.usual_rows
FROM per_file f
JOIN usual u USING (base_time)
WHERE f.loaded_rows <> u.usual_rows
ORDER BY 1, 2, 3, 4
