package com.orbit.scheduler.dialect;

/**
 * 调度框架数据库方言。
 *
 * <p>同一构建产物（同一 JAR）可不作任何代码改动运行在 PostgreSQL 与 GaussDB 之上：
 *
 * <table border="1">
 * <tr><th>差异点</th><th>PostgreSQL</th><th>GaussDB</th><th>框架抹平手段</th></tr>
 * <tr><td>Upsert 语法</td><td>ON CONFLICT DO UPDATE</td><td>MERGE INTO</td>
 *     <td>DAO 统一 UPDATE→INSERT 兜底，不使用任何 upsert 方言</td></tr>
 * <tr><td>自增主键</td><td>BIGSERIAL + getGeneratedKeys</td>
 *     <td>SEQUENCE + nextval 预取（显式 id 插入）</td>
 *     <td>按方言选择主键生成策略，见 {@link #prefetchIdsFromSequence()}</td></tr>
 * <tr><td>INSERT 回填</td><td>RETURNING / getGeneratedKeys 可用</td>
 *     <td>RETURNING 行为存在差异，不依赖</td>
 *     <td>GaussDB 走 SELECT nextval 预取，彻底规避 RETURNING</td></tr>
 * <tr><td>空字符串</td><td>'' ≠ NULL</td><td>'' ≡ NULL</td>
 *     <td>写库前统一"空串→NULL"归一化 + task_name 强制非空校验</td></tr>
 * <tr><td>Quartz Delegate</td><td>PostgreSQLDelegate</td><td>继承即可用</td>
 *     <td>GaussDBDelegate extends PostgreSQLDelegate，自动按方言注入</td></tr>
 * </table>
 *
 * <p>其余 BYTEA / BOOLEAN / TIMESTAMP / TEXT / LIMIT-OFFSET / SELECT FOR UPDATE
 * 双库语义一致，框架全部 SQL 均收敛在该子集内；
 * 时间值一律由应用侧计算后以参数传入，SQL 中不使用任何数据库日期函数。
 *
 * @author orbit
 */
public enum SchedulerDialect {

    /** PostgreSQL 原生：主键走 BIGSERIAL + getGeneratedKeys（显式指定回填列名） */
    POSTGRESQL("PostgreSQL",
            "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate",
            true,
            false),

    /**
     * 华为 GaussDB（openGauss 内核）。
     * 主键走显式 SEQUENCE + nextval 预取（规避 RETURNING / getGeneratedKeys 行为差异）；
     * 空字符串写入退化为 NULL，DAO 层已统一归一化。
     */
    GAUSSDB("GaussDB",
            "com.orbit.scheduler.quartz.GaussDBDelegate",
            false,
            true),

    /** 其他数据库（MySQL / H2 等）：框架不注入 Quartz delegate，沿用用户 yaml 配置 */
    OTHER("Other(MySQL/H2/...)",
            null,
            true,
            false);

    private final String displayName;
    private final String quartzDelegateClassName;
    /** 空字符串是否独立于 NULL（GaussDB 下 '' 会退化为 NULL） */
    private final boolean emptyStringDistinctFromNull;
    /** 是否通过 SELECT nextval 预取主键、以显式 id 插入（规避 RETURNING/getGeneratedKeys） */
    private final boolean prefetchIdsFromSequence;

    SchedulerDialect(String displayName, String quartzDelegateClassName,
                     boolean emptyStringDistinctFromNull, boolean prefetchIdsFromSequence) {
        this.displayName = displayName;
        this.quartzDelegateClassName = quartzDelegateClassName;
        this.emptyStringDistinctFromNull = emptyStringDistinctFromNull;
        this.prefetchIdsFromSequence = prefetchIdsFromSequence;
    }

    public String getDisplayName() {
        return displayName;
    }

    /** Quartz driverDelegateClass；null 表示框架不干预（由用户配置决定） */
    public String getQuartzDelegateClassName() {
        return quartzDelegateClassName;
    }

    /** 是否需要框架自动注入 Quartz driverDelegateClass */
    public boolean injectsQuartzDelegate() {
        return quartzDelegateClassName != null;
    }

    public boolean isEmptyStringDistinctFromNull() {
        return emptyStringDistinctFromNull;
    }

    /**
     * 是否采用 SEQUENCE 预取主键策略：先 SELECT nextval('seq_xxx') 取得主键，
     * 再以显式 id 执行 INSERT。
     *
     * <p>GaussDB 下 INSERT ... RETURNING / getGeneratedKeys 的行为与 PostgreSQL
     * 存在差异，预取方式仅依赖普通 SELECT + 普通 INSERT，是最稳妥的主键获取方式；
     * PostgreSQL / MySQL / H2 保持 getGeneratedKeys（指定回填列名）不变。
     */
    public boolean prefetchIdsFromSequence() {
        return prefetchIdsFromSequence;
    }
}
