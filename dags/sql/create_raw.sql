CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.vilage_fcst (
    base_date    text,
    base_time    text,
    nx           text,
    ny           text,
    category     text,
    fcst_date    text,
    fcst_time    text,
    fcst_value   text,
    s3_key       text        NOT NULL, 
    loaded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_vilage_fcst_file
    ON raw.vilage_fcst (base_date, base_time, nx, ny);

CREATE TABLE IF NOT EXISTS raw.load_log (
    base_date      text,
    base_time      text,
    nx             text,
    ny             text,
    s3_key         text,
    expected_rows  integer,
    loaded_rows    integer,
    status         text NOT NULL
        CHECK (status IN ('loaded', 'count_mismatch', 'source_missing', 'error')),
    error          text,
    loaded_at      timestamptz  NOT NULL DEFAULT now(),
    fetched_at     timestamptz,
    PRIMARY KEY (base_date, base_time, nx, ny)
);

CREATE TABLE IF NOT EXISTS raw.asos_hourly (
    stn_id     text,
    tm         text,
    rn         text,
    rn_qcflg   text,
    ta         text,
    ta_qcflg   text,
    s3_key     text        NOT NULL,
    loaded_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_asos_hourly_s3_key
    ON raw.asos_hourly (s3_key);

CREATE TABLE IF NOT EXISTS raw.asos_load_log (
    obs_date       text,
    stn_id         text,
    s3_key         text,
    expected_rows  integer,
    loaded_rows    integer,
    status         text NOT NULL
        CHECK (status IN ('loaded', 'count_mismatch', 'source_missing', 'error')),
    error          text,
    fetched_at     timestamptz,
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (obs_date, stn_id)
);