-- ===========================================================================
-- Orbit Batch Scheduler :: 增量迁移 v1.1（跨服务批量调度）
-- 适用：已有 t_job_config 表的环境（PostgreSQL / GaussDB / MySQL / H2）
-- 新增列：http_method（REMOTE HTTP 方法）、workflow_def（WORKFLOW 编排 JSON）
-- 幂等：列已存在时请忽略对应错误，或按方言使用 IF NOT EXISTS 变体
-- ===========================================================================

-- ---------- PostgreSQL / GaussDB ----------
-- ALTER TABLE t_job_config ADD COLUMN IF NOT EXISTS http_method VARCHAR(16);
-- ALTER TABLE t_job_config ADD COLUMN IF NOT EXISTS workflow_def TEXT;

-- ---------- MySQL 8 ----------
-- ALTER TABLE t_job_config ADD COLUMN http_method VARCHAR(16) NULL COMMENT 'REMOTE HTTP方法' AFTER http_path;
-- ALTER TABLE t_job_config ADD COLUMN workflow_def TEXT NULL COMMENT 'WORKFLOW编排定义JSON' AFTER params;

-- ---------- 通用（无 IF NOT EXISTS 时逐条执行，已存在则跳过） ----------
ALTER TABLE t_job_config ADD COLUMN http_method VARCHAR(16);
ALTER TABLE t_job_config ADD COLUMN workflow_def TEXT;
