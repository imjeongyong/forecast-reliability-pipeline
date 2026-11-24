WITH l AS (
    SELECT base_date, base_time, nx, ny, loaded_rows
    FROM raw.load_log
    WHERE status = 'loaded'
),
t AS (
    SELECT base_date, base_time, nx, ny, count(*) AS table_rows
    FROM raw.vilage_fcst
    GROUP BY base_date, base_time, nx, ny
)
SELECT base_date, base_time, nx, ny, l.loaded_rows, t.table_rows
FROM l
FULL JOIN t USING (base_date, base_time, nx, ny)
WHERE l.loaded_rows IS DISTINCT FROM t.table_rows
ORDER BY 1, 2, 3, 4
