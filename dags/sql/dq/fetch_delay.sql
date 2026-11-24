WITH f AS (
    SELECT base_date, base_time, nx, ny, fetched_at,
           to_timestamp(base_date || base_time, 'YYYYMMDDHH24MI')::timestamp
               AT TIME ZONE 'Asia/Seoul' AS issued_at
    FROM raw.load_log
    WHERE status = 'loaded'
)
SELECT base_date, base_time, nx, ny, fetched_at, issued_at,
       fetched_at - issued_at AS delay
FROM f
WHERE fetched_at > issued_at + interval '30 minutes'
  AND issued_at >= timestamptz '2026-11-16 02:00+09'
ORDER BY 1, 2, 3, 4