-- Orbit Admin 业务表（PostgreSQL / GaussDB / H2 兼容）
CREATE TABLE IF NOT EXISTS orbit_job (
    id               BIGSERIAL PRIMARY KEY,
    job_name         VARCHAR(64)  NOT NULL,
    description      VARCHAR(256),
    app_name         VARCHAR(64)  NOT NULL,
    handler          VARCHAR(128) NOT NULL,
    cron_expr        VARCHAR(64),
    params           VARCHAR(2000),
    timeout_seconds  INT DEFAULT 300,
    route_strategy   VARCHAR(16) DEFAULT 'ROUND',
    enabled          BOOLEAN DEFAULT TRUE,
    version          INT DEFAULT 1,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    CONSTRAINT uk_orbit_job_name UNIQUE (job_name)
);

CREATE TABLE IF NOT EXISTS orbit_job_log (
    id                BIGSERIAL PRIMARY KEY,
    log_id            VARCHAR(64) NOT NULL,
    job_id            BIGINT,
    job_name          VARCHAR(64),
    app_name          VARCHAR(64),
    handler           VARCHAR(128),
    executor_address  VARCHAR(256),
    status            VARCHAR(16),
    message           VARCHAR(2000),
    cost_ms           BIGINT,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orbit_job_log_name ON orbit_job_log (job_name);
CREATE INDEX IF NOT EXISTS idx_orbit_job_log_id ON orbit_job_log (log_id);

-- 执行器心跳注册表（对齐 XXL-JOB xxl_job_registry）：共享库即可无状态 Deployment
CREATE TABLE IF NOT EXISTS orbit_executor_registry (
    id               BIGSERIAL PRIMARY KEY,
    app_name         VARCHAR(64)  NOT NULL,
    address          VARCHAR(256) NOT NULL,
    node_id          VARCHAR(128),
    handlers         VARCHAR(2000),
    last_heartbeat   TIMESTAMP    NOT NULL,
    CONSTRAINT uk_orbit_executor_app_addr UNIQUE (app_name, address)
);

CREATE INDEX IF NOT EXISTS idx_orbit_executor_hb ON orbit_executor_registry (last_heartbeat);
