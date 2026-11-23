WITH expected AS (
    SELECT to_char(ts, 'YYYYMMDD') AS base_date,
           to_char(ts, 'HH24MI')   AS base_time
    FROM generate_series(
             timestamp '2026-11-15 23:00',
             (now() AT TIME ZONE 'Asia/Seoul') - interval '90 minutes',
             interval '1 hour'
         ) AS ts
    WHERE extract(hour FROM ts) IN (2, 5, 8, 11, 14, 17, 20, 23)
),
actual AS (
    SELECT DISTINCT base_date, base_time
    FROM raw.load_log
)
SELECT e.base_date, e.base_time
FROM expected e
LEFT JOIN actual a USING (base_date, base_time)
WHERE a.base_date IS NULL
ORDER BY 1, 2