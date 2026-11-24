CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.check_result (
    run_id text NOT NULL,
    check_name text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('error', 'warn')),
    status text NOT NULL CHECK (status IN ('pass', 'fail')),
    failed_count integer NOT NULL,
    sample jsonb,
    checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);
