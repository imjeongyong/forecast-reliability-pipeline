SELECT base_date, base_time, nx, ny, category, fcst_date, fcst_time,
       count(*) AS cnt
FROM raw.vilage_fcst
GROUP BY base_date, base_time, nx, ny, category, fcst_date, fcst_time
HAVING count(*) > 1
ORDER BY 1, 2, 3, 4, 5, 6, 7
