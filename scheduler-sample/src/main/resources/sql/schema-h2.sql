-- ===========================================================================
-- Orbit Batch Scheduler :: H2 内存库初始化脚本（local profile）
-- Quartz 11 张表由 spring.quartz.jdbc.initialize-schema=always 自动创建
-- 本脚本仅创建业务表 + 演示种子数据
-- ===========================================================================

CREATE TABLE IF NOT EXISTS t_job_config (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_name         VARCHAR(128)  NOT NULL,
    task_group        VARCHAR(64)   DEFAULT 'ORBIT',
    description       VARCHAR(512),
    cron_expression   VARCHAR(64),
    dispatch_type     VARCHAR(16)   NOT NULL DEFAULT 'LOCAL',
    http_service_name VARCHAR(128),
    http_path         VARCHAR(256),
    http_method       VARCHAR(16),
    timeout_seconds   INT           DEFAULT 300,
    params            TEXT,
    workflow_def      TEXT,
    enabled           TINYINT(1)    DEFAULT 1,
    version           INT           DEFAULT 1,
    created_at        DATETIME(3),
    updated_at        DATETIME(3),
    CONSTRAINT uk_job_config_task_name UNIQUE (task_name)
);

CREATE TABLE IF NOT EXISTS t_job_log (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_id    VARCHAR(64),
    task_name     VARCHAR(128) NOT NULL,
    task_group    VARCHAR(64),
    dispatch_type VARCHAR(16),
    dispatch_node VARCHAR(128),
    worker_node   VARCHAR(128),
    status        VARCHAR(16)  NOT NULL,
    start_time    DATETIME(3),
    end_time      DATETIME(3),
    cost_ms       BIGINT,
    message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_log_task_start ON t_job_log (task_name, start_time);

-- ---------------------------------------------------------------------------
-- 种子数据：DB 配置覆盖注解默认 cron 的演示（remoteDataSync 注解默认 5 分钟，
-- 此处改为 3 分钟；若同名行已存在则跳过 —— 注解种子逻辑只在缺失时插入）
-- ---------------------------------------------------------------------------
INSERT INTO t_job_config
    (task_name, task_group, description, cron_expression, dispatch_type, timeout_seconds, params, enabled, version, created_at, updated_at)
SELECT
    'remoteDataSync', 'ORBIT', '跨节点数据同步（HTTP派发演示，cron由DB覆盖为3分钟）', '0 */3 * * * ?', 'HTTP', 300, '{"bizDate":"auto"}', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM t_job_config WHERE task_name = 'remoteDataSync');
