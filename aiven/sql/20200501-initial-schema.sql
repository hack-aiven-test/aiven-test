CREATE TABLE IF NOT EXISTS monmon_website
    (
        id UUID PRIMARY KEY,
        -- https://tools.ietf.org/html/rfc1035
        -- 2.3.4. Size limits
        website VARCHAR(255) NOT NULL UNIQUE,
    );

CREATE TABLE IF NOT EXISTS monmon_website_metrics
    (
        id uuid PRIMARY KEY,
        website_fk UUID REFERENCES monmon_website (id),
        measurement_started_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        -- These fields are nullable because the measurement may fail
        http_status_code INT NULL,
        http_fully_loaded_time_ms INT NULL,
    );
