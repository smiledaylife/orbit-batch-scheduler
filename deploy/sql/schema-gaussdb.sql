-- ===========================================================================
-- Orbit Batch Scheduler :: GaussDB 初始化脚本（gaussdb profile）
-- 适用：华为 GaussDB（openGauss 内核）/ openGauss 社区版
-- 包含：Quartz 2.5.x 集群表（PostgreSQL 官方结构）+ 框架业务表 + 演示种子
-- 幂等：可重复执行（CREATE TABLE IF NOT EXISTS / DROP INDEX IF EXISTS /
--       NOT EXISTS 种子插入 / CREATE SEQUENCE IF NOT EXISTS）
--
-- 建库（DBA 一次性执行）：
--   gsql -d postgres -c "CREATE DATABASE orbit_scheduler DBCOMPATIBILITY = 'A' ENCODING 'UTF8';"
--   gsql -d orbit_scheduler -f schema-gaussdb.sql
--
-- 执行方式：逐条自动提交执行（gsql 默认自动提交）。GaussDB 的 DDL 为隐式
-- 提交、不可回滚，请勿将本脚本包裹在 BEGIN ... ROLLBACK 事务块中执行。
--
-- ---------------------------------------------------------------------------
-- 【GaussDB 与 PostgreSQL 的语法差异及本框架的抹平方式】
-- （已对照 PG → GaussDB 语法差异评估清单逐项核对）
--
--  1) 空字符串退化为 NULL：
--     框架 DAO 写库前统一"空串→NULL"归一化 + task_name 强制非空校验，
--     三库（PG/GaussDB/MySQL）行为完全一致，业务无需感知。
--  2) SERIAL/BIGSERIAL 需改写：
--     本脚本业务表主键不用 BIGSERIAL，统一改写为
--     CREATE SEQUENCE + DEFAULT nextval('seq')（见下方 t_job_config/t_job_log）；
--     应用侧 GaussDB 方言走"SELECT nextval 预取 + 显式 id 插入"，
--     完全不依赖 INSERT 回填机制。
--  3) INSERT ... RETURNING / getGeneratedKeys 行为存在差异：
--     GaussDB 方言的主键获取改为 SELECT nextval('seq') 预取（普通 SELECT），
--     规避 RETURNING 子句；序列缺失时自动降级生成键回填并输出 WARN 日志。
--  4) Upsert 语法不同（PG 为 ON CONFLICT，GaussDB 为 MERGE INTO）：
--     框架不使用任何 upsert 方言（DAO 统一 UPDATE→INSERT 兜底）。
--  5) VARCHAR(n) 按字节计数（UTF-8 下中文占 3 字节）：
--     本脚本已将含中文可能的字段长度放大（description 1024、task_group 128、
--     Quartz DESCRIPTION 750），纯 ASCII 标识列保持原长。
--  6) DATE 类型含时间部分：
--     全部时间列统一使用 TIMESTAMP，不用 DATE；时间值由应用侧计算后
--     以参数传入，SQL 中不出现 CURRENT_DATE/DATE_TRUNC/AGE 等日期函数。
--  7) DDL 隐式提交、不可回滚：
--     初始化脚本逐条自动提交执行，幂等可重复；勿包事务。
--  8) 类型转换运算符 :: 在部分场景受限：
--     框架 SQL 全部使用标准 CAST() 或参数绑定，不使用 ::。
--  9) FILTER (WHERE) / DISTINCT ON / LATERAL / LISTEN-NOTIFY / JSONB 操作符
--     等差异项：框架 SQL 均未使用（收敛于双库公共子集）。
-- 10) Quartz 驱动适配器：框架按方言自动注入 GaussDBDelegate
--     （继承官方 PostgreSQLDelegate，BYTEA/BOOLEAN/FOR UPDATE 语义一致），
--     无需在 spring.quartz.properties 配置 driverDelegateClass。
--
-- 驱动建议：org.opengauss:opengauss-jdbc（org.opengauss.Driver，
--           jdbc:opengauss://host:port/db）；华为云 GaussDB 亦可用官方
--           gaussdbjdbc（com.huawei.gaussdb.jdbc.Driver，jdbc:gaussdb://host:port/db）。
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Quartz 集群表（与 schema-postgresql.sql 结构完全一致；openGauss 支持
-- BOOL / BYTEA / SMALLINT / NUMERIC 等 PG 内核类型，与兼容模式无关）
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS QRTZ_JOB_DETAILS (
    SCHED_NAME        VARCHAR(120) NOT NULL,
    JOB_NAME          VARCHAR(200) NOT NULL,
    JOB_GROUP         VARCHAR(200) NOT NULL,
    DESCRIPTION       VARCHAR(750) NULL,
    JOB_CLASS_NAME    VARCHAR(250) NOT NULL,
    IS_DURABLE        BOOL         NOT NULL,
    IS_NONCONCURRENT  BOOL         NOT NULL,
    IS_UPDATE_DATA    BOOL         NOT NULL,
    REQUESTS_RECOVERY BOOL         NOT NULL,
    JOB_DATA          BYTEA        NULL,
    PRIMARY KEY (SCHED_NAME, JOB_NAME, JOB_GROUP)
);

CREATE TABLE IF NOT EXISTS QRTZ_TRIGGERS (
    SCHED_NAME     VARCHAR(120) NOT NULL,
    TRIGGER_NAME   VARCHAR(200) NOT NULL,
    TRIGGER_GROUP  VARCHAR(200) NOT NULL,
    JOB_NAME       VARCHAR(200) NOT NULL,
    JOB_GROUP      VARCHAR(200) NOT NULL,
    DESCRIPTION    VARCHAR(750) NULL,
    NEXT_FIRE_TIME BIGINT       NULL,
    PREV_FIRE_TIME BIGINT       NULL,
    PRIORITY       INTEGER      NULL,
    TRIGGER_STATE  VARCHAR(16)  NOT NULL,
    TRIGGER_TYPE   VARCHAR(8)   NOT NULL,
    START_TIME     BIGINT       NOT NULL,
    END_TIME       BIGINT       NULL,
    CALENDAR_NAME  VARCHAR(200) NULL,
    MISFIRE_INSTR  SMALLINT     NULL,
    JOB_DATA       BYTEA        NULL,
    PRIMARY KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    FOREIGN KEY (SCHED_NAME, JOB_NAME, JOB_GROUP)
        REFERENCES QRTZ_JOB_DETAILS (SCHED_NAME, JOB_NAME, JOB_GROUP)
);

CREATE TABLE IF NOT EXISTS QRTZ_SIMPLE_TRIGGERS (
    SCHED_NAME      VARCHAR(120) NOT NULL,
    TRIGGER_NAME    VARCHAR(200) NOT NULL,
    TRIGGER_GROUP   VARCHAR(200) NOT NULL,
    REPEAT_COUNT    BIGINT       NOT NULL,
    REPEAT_INTERVAL BIGINT       NOT NULL,
    TIMES_TRIGGERED BIGINT       NOT NULL,
    PRIMARY KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
        REFERENCES QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
);

CREATE TABLE IF NOT EXISTS QRTZ_CRON_TRIGGERS (
    SCHED_NAME      VARCHAR(120) NOT NULL,
    TRIGGER_NAME    VARCHAR(200) NOT NULL,
    TRIGGER_GROUP   VARCHAR(200) NOT NULL,
    CRON_EXPRESSION VARCHAR(120) NOT NULL,
    TIME_ZONE_ID    VARCHAR(80),
    PRIMARY KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
        REFERENCES QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
);

CREATE TABLE IF NOT EXISTS QRTZ_SIMPROP_TRIGGERS (
    SCHED_NAME    VARCHAR(120)   NOT NULL,
    TRIGGER_NAME  VARCHAR(200)   NOT NULL,
    TRIGGER_GROUP VARCHAR(200)   NOT NULL,
    STR_PROP_1    VARCHAR(512)   NULL,
    STR_PROP_2    VARCHAR(512)   NULL,
    STR_PROP_3    VARCHAR(512)   NULL,
    INT_PROP_1    INTEGER        NULL,
    INT_PROP_2    INTEGER        NULL,
    LONG_PROP_1   BIGINT         NULL,
    LONG_PROP_2   BIGINT         NULL,
    DEC_PROP_1    NUMERIC(13, 4) NULL,
    DEC_PROP_2    NUMERIC(13, 4) NULL,
    BOOL_PROP_1   BOOL           NULL,
    BOOL_PROP_2   BOOL           NULL,
    PRIMARY KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
        REFERENCES QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
);

CREATE TABLE IF NOT EXISTS QRTZ_BLOB_TRIGGERS (
    SCHED_NAME    VARCHAR(120) NOT NULL,
    TRIGGER_NAME  VARCHAR(200) NOT NULL,
    TRIGGER_GROUP VARCHAR(200) NOT NULL,
    BLOB_DATA     BYTEA        NULL,
    PRIMARY KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
        REFERENCES QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
);

CREATE TABLE IF NOT EXISTS QRTZ_CALENDARS (
    SCHED_NAME    VARCHAR(120) NOT NULL,
    CALENDAR_NAME VARCHAR(200) NOT NULL,
    CALENDAR      BYTEA        NOT NULL,
    PRIMARY KEY (SCHED_NAME, CALENDAR_NAME)
);

CREATE TABLE IF NOT EXISTS QRTZ_PAUSED_TRIGGER_GRPS (
    SCHED_NAME    VARCHAR(120) NOT NULL,
    TRIGGER_GROUP VARCHAR(200) NOT NULL,
    PRIMARY KEY (SCHED_NAME, TRIGGER_GROUP)
);

CREATE TABLE IF NOT EXISTS QRTZ_FIRED_TRIGGERS (
    SCHED_NAME        VARCHAR(120) NOT NULL,
    ENTRY_ID          VARCHAR(95)  NOT NULL,
    TRIGGER_NAME      VARCHAR(200) NOT NULL,
    TRIGGER_GROUP     VARCHAR(200) NOT NULL,
    INSTANCE_NAME     VARCHAR(200) NOT NULL,
    FIRED_TIME        BIGINT       NOT NULL,
    SCHED_TIME        BIGINT       NOT NULL,
    PRIORITY          INTEGER      NOT NULL,
    STATE             VARCHAR(16)  NOT NULL,
    JOB_NAME          VARCHAR(200) NULL,
    JOB_GROUP         VARCHAR(200) NULL,
    IS_NONCONCURRENT  BOOL         NOT NULL,
    REQUESTS_RECOVERY BOOL         NULL,
    PRIMARY KEY (SCHED_NAME, ENTRY_ID)
);

CREATE TABLE IF NOT EXISTS QRTZ_SCHEDULER_STATE (
    SCHED_NAME        VARCHAR(120) NOT NULL,
    INSTANCE_NAME     VARCHAR(200) NOT NULL,
    LAST_CHECKIN_TIME BIGINT       NOT NULL,
    CHECKIN_INTERVAL  BIGINT       NOT NULL,
    PRIMARY KEY (SCHED_NAME, INSTANCE_NAME)
);

CREATE TABLE IF NOT EXISTS QRTZ_LOCKS (
    SCHED_NAME VARCHAR(120) NOT NULL,
    LOCK_NAME  VARCHAR(40)  NOT NULL,
    PRIMARY KEY (SCHED_NAME, LOCK_NAME)
);

-- 注：索引采用 DROP+CREATE 保证幂等（部分 GaussDB 版本不支持 CREATE INDEX IF NOT EXISTS）
DROP INDEX IF EXISTS IDX_QRTZ_J_REQ_RECOVERY;
CREATE INDEX IDX_QRTZ_J_REQ_RECOVERY ON QRTZ_JOB_DETAILS (SCHED_NAME, REQUESTS_RECOVERY);
DROP INDEX IF EXISTS IDX_QRTZ_J_GRP;
CREATE INDEX IDX_QRTZ_J_GRP ON QRTZ_JOB_DETAILS (SCHED_NAME, JOB_GROUP);
DROP INDEX IF EXISTS IDX_QRTZ_T_J;
CREATE INDEX IDX_QRTZ_T_J ON QRTZ_TRIGGERS (SCHED_NAME, JOB_NAME, JOB_GROUP);
DROP INDEX IF EXISTS IDX_QRTZ_T_JG;
CREATE INDEX IDX_QRTZ_T_JG ON QRTZ_TRIGGERS (SCHED_NAME, JOB_GROUP);
DROP INDEX IF EXISTS IDX_QRTZ_T_C;
CREATE INDEX IDX_QRTZ_T_C ON QRTZ_TRIGGERS (SCHED_NAME, CALENDAR_NAME);
DROP INDEX IF EXISTS IDX_QRTZ_T_G;
CREATE INDEX IDX_QRTZ_T_G ON QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_GROUP);
DROP INDEX IF EXISTS IDX_QRTZ_T_STATE;
CREATE INDEX IDX_QRTZ_T_STATE ON QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_STATE);
DROP INDEX IF EXISTS IDX_QRTZ_T_N_STATE;
CREATE INDEX IDX_QRTZ_T_N_STATE ON QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, TRIGGER_STATE);
DROP INDEX IF EXISTS IDX_QRTZ_T_N_G_STATE;
CREATE INDEX IDX_QRTZ_T_N_G_STATE ON QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_GROUP, TRIGGER_STATE);
DROP INDEX IF EXISTS IDX_QRTZ_T_NEXT_FIRE_TIME;
CREATE INDEX IDX_QRTZ_T_NEXT_FIRE_TIME ON QRTZ_TRIGGERS (SCHED_NAME, NEXT_FIRE_TIME);
DROP INDEX IF EXISTS IDX_QRTZ_T_NFT_ST;
CREATE INDEX IDX_QRTZ_T_NFT_ST ON QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_STATE, NEXT_FIRE_TIME);
DROP INDEX IF EXISTS IDX_QRTZ_T_NFT_MISFIRE;
CREATE INDEX IDX_QRTZ_T_NFT_MISFIRE ON QRTZ_TRIGGERS (SCHED_NAME, MISFIRE_INSTR, NEXT_FIRE_TIME);
DROP INDEX IF EXISTS IDX_QRTZ_T_NFT_ST_MISFIRE;
CREATE INDEX IDX_QRTZ_T_NFT_ST_MISFIRE ON QRTZ_TRIGGERS (SCHED_NAME, MISFIRE_INSTR, NEXT_FIRE_TIME, TRIGGER_STATE);
DROP INDEX IF EXISTS IDX_QRTZ_T_NFT_ST_MISFIRE_GRP;
CREATE INDEX IDX_QRTZ_T_NFT_ST_MISFIRE_GRP ON QRTZ_TRIGGERS (SCHED_NAME, MISFIRE_INSTR, NEXT_FIRE_TIME, TRIGGER_GROUP, TRIGGER_STATE);
DROP INDEX IF EXISTS IDX_QRTZ_FT_TRIG_INST_NAME;
CREATE INDEX IDX_QRTZ_FT_TRIG_INST_NAME ON QRTZ_FIRED_TRIGGERS (SCHED_NAME, INSTANCE_NAME);
DROP INDEX IF EXISTS IDX_QRTZ_FT_INST_JOB_REQ_RCVRY;
CREATE INDEX IDX_QRTZ_FT_INST_JOB_REQ_RCVRY ON QRTZ_FIRED_TRIGGERS (SCHED_NAME, REQUESTS_RECOVERY);
DROP INDEX IF EXISTS IDX_QRTZ_FT_J_G;
CREATE INDEX IDX_QRTZ_FT_J_G ON QRTZ_FIRED_TRIGGERS (SCHED_NAME, JOB_NAME, JOB_GROUP);
DROP INDEX IF EXISTS IDX_QRTZ_FT_JG;
CREATE INDEX IDX_QRTZ_FT_JG ON QRTZ_FIRED_TRIGGERS (SCHED_NAME, JOB_GROUP);
DROP INDEX IF EXISTS IDX_QRTZ_FT_T_G;
CREATE INDEX IDX_QRTZ_FT_T_G ON QRTZ_FIRED_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP);
DROP INDEX IF EXISTS IDX_QRTZ_FT_TG;
CREATE INDEX IDX_QRTZ_FT_TG ON QRTZ_FIRED_TRIGGERS (SCHED_NAME, TRIGGER_GROUP);

-- ---------------------------------------------------------------------------
-- 框架业务表
-- 主键不使用 BIGSERIAL（GaussDB 下需改写）：显式 SEQUENCE + DEFAULT nextval；
-- 应用侧 GaussDB 方言采用 SELECT nextval 预取 + 显式 id 插入（见 JdbcTaskRepository）
-- VARCHAR 长度按字节语义放大（UTF-8 中文 3 字节/字）
-- ---------------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS seq_job_config_id START WITH 1;
-- 注：若目标 GaussDB 版本不支持 CREATE SEQUENCE IF NOT EXISTS，请改执行：
--   CREATE SEQUENCE seq_job_config_id START WITH 1;
CREATE TABLE IF NOT EXISTS t_job_config (
    id                BIGINT NOT NULL DEFAULT nextval('seq_job_config_id') PRIMARY KEY,
    task_name         VARCHAR(128) NOT NULL,
    task_group        VARCHAR(128) DEFAULT 'ORBIT',
    description       VARCHAR(1024),
    cron_expression   VARCHAR(64),
    dispatch_type     VARCHAR(16) NOT NULL DEFAULT 'LOCAL',
    http_service_name VARCHAR(128),
    http_path         VARCHAR(256),
    timeout_seconds   INT DEFAULT 300,
    params            TEXT,
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    version           INT DEFAULT 1,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_job_config_task_name UNIQUE (task_name)
);

CREATE SEQUENCE IF NOT EXISTS seq_job_log_id START WITH 1;
CREATE TABLE IF NOT EXISTS t_job_log (
    id            BIGINT NOT NULL DEFAULT nextval('seq_job_log_id') PRIMARY KEY,
    request_id    VARCHAR(64),
    task_name     VARCHAR(128) NOT NULL,
    task_group    VARCHAR(128),
    dispatch_type VARCHAR(16),
    dispatch_node VARCHAR(128),
    worker_node   VARCHAR(128),
    status        VARCHAR(16) NOT NULL,
    start_time    TIMESTAMP,
    end_time      TIMESTAMP,
    cost_ms       BIGINT,
    message       TEXT
);
DROP INDEX IF EXISTS idx_job_log_task_start;
CREATE INDEX idx_job_log_task_start ON t_job_log (task_name, start_time);
DROP INDEX IF EXISTS idx_job_log_status;
CREATE INDEX idx_job_log_status ON t_job_log (status);

CREATE TABLE IF NOT EXISTS t_cluster_lock (
    lock_name   VARCHAR(128) NOT NULL,
    owner       VARCHAR(128),
    expire_at   BIGINT,
    update_time BIGINT,
    PRIMARY KEY (lock_name)
);

-- ---------------------------------------------------------------------------
-- 演示种子数据（幂等；时间列走 DDL 默认值，规避方言时间函数差异；
-- id 依赖 DEFAULT nextval 自动生成）
-- ---------------------------------------------------------------------------
INSERT INTO t_job_config
    (task_name, task_group, description, cron_expression, dispatch_type, timeout_seconds, params, enabled, version)
SELECT 'remoteDataSync', 'ORBIT', '跨节点数据同步（HTTP派发演示，cron由DB覆盖为3分钟）',
       '0 */3 * * * ?', 'HTTP', 300, '{"bizDate":"auto"}', TRUE, 1
WHERE NOT EXISTS (SELECT 1 FROM t_job_config WHERE task_name = 'remoteDataSync');

INSERT INTO t_job_config
    (task_name, task_group, description, cron_expression, dispatch_type, timeout_seconds, params, enabled, version)
SELECT 'futureTask', 'ORBIT', '预留任务（禁用状态示例，可随时启用）',
       '0 0 5 * * ?', 'LOCAL', 600, '{}', FALSE, 1
WHERE NOT EXISTS (SELECT 1 FROM t_job_config WHERE task_name = 'futureTask');

-- ---------------------------------------------------------------------------
-- 附录：PostgreSQL 脚本（schema-postgresql.sql）使用 BIGSERIAL，本脚本为
-- SEQUENCE 等价改写。两套表结构列名/类型语义完全一致，同一套 DAO 双库运行：
--   PostgreSQL  → BIGSERIAL + getGeneratedKeys 回填主键
--   GaussDB     → SEQUENCE + SELECT nextval 预取主键（显式 id 插入）
-- 手工插入数据时可不指定 id（走 DEFAULT nextval）。
-- ---------------------------------------------------------------------------
