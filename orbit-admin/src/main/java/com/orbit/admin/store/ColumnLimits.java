package com.orbit.admin.store;

/**
 * 数据库列宽上限的唯一事实来源，取值与 {@code src/main/resources/schema.sql}
 * 以及 {@code deploy/sql/schema-*.sql} 的 VARCHAR 定义一一对应。
 * <p>
 * 所有写入路径（{@code JobStore}、{@code JobService}、{@code ExecutorRegistry}）统一引用本类，
 * 同一个宽度不再散落在多处；调整列宽时只需同步修改本类与建表脚本。
 * <p>
 * 这些上限都在<b>入库之前</b>校验：超长值若等到 INSERT/UPDATE 才失败，
 * 会被全局异常处理器压成一句没有信息的 {@code internal error}，调用方看不出是哪个入参的问题。
 */
public final class ColumnLimits {

    // ==================== orbit_job ====================

    /** {@code orbit_job.description} */
    public static final int JOB_DESCRIPTION = 256;

    /** {@code orbit_job.app_name} */
    public static final int JOB_APP_NAME = 64;

    /** {@code orbit_job.handler} */
    public static final int JOB_HANDLER = 128;

    /** {@code orbit_job.cron_expr} */
    public static final int JOB_CRON_EXPR = 64;

    /** {@code orbit_job.params}（存的是 JSON 序列化结果） */
    public static final int JOB_PARAMS_JSON = 2000;

    // ==================== orbit_job_log ====================

    /** {@code orbit_job_log.message} */
    public static final int LOG_MESSAGE = 2000;

    // ==================== orbit_executor_registry ====================

    /** {@code orbit_executor_registry.app_name} */
    public static final int REG_APP_NAME = 64;

    /** {@code orbit_executor_registry.address} */
    public static final int REG_ADDRESS = 256;

    /** {@code orbit_executor_registry.node_id} */
    public static final int REG_NODE_ID = 128;

    /** {@code orbit_executor_registry.handlers}（存的是 JSON 数组字符串） */
    public static final int REG_HANDLERS_JSON = 2000;

    private ColumnLimits() {
    }

    /**
     * 校验字段长度不超过列宽，超限时抛出带字段名与上限的 {@link IllegalArgumentException}
     * （由全局异常处理器映射为 400）。
     *
     * @param field 字段名，仅用于错误信息
     * @param value 待校验的值，{@code null} 视为通过
     * @param max   列宽上限
     */
    public static void requireMaxLength(String field, String value, int max) {
        if (value != null && value.length() > max) {
            throw new IllegalArgumentException(field + " too long: length " + value.length()
                    + " exceeds limit " + max);
        }
    }

    /**
     * 把字符串截断到列宽以内，超长时以 {@code ...} 结尾。
     * 结果总长度（含省略号）不超过 {@code max}，因此截断后的值一定能安全入库。
     *
     * @param value 待截断的值，{@code null} 原样返回
     * @param max   列宽上限
     * @return 长度不超过 {@code max} 的字符串
     */
    public static String abbreviate(String value, int max) {
        if (value == null || value.length() <= max) {
            return value;
        }
        return value.substring(0, max - 3) + "...";
    }
}
