WITH f AS (
    SELECT base_date, base_time, nx, ny, fetched_at,
           to_timestamp(base_date || base_time, 'YYYYMMDDHH24MI')::timestamp
               AT TIME ZONE 'Asia/Seoul' AS issued_at
    FROM raw.load_log
    WHERE status = 'loaded'
)
SELECT base_date, base_time, nx, ny, fetched_at, issued_at
FROM f
WHERE fetched_at < issued_at
ORDER BY 1, 2, 3, 4
