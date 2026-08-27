package com.orbit.scheduler.quartz;

import org.quartz.impl.jdbcjobstore.PostgreSQLDelegate;

/**
 * Quartz JDBC JobStore 驱动适配器：华为 GaussDB（openGauss 内核）。
 *
 * <h3>适配说明</h3>
 * <p>openGauss 内核源自 PostgreSQL 9.2，Quartz 所依赖的存储能力在 GaussDB 上
 * 与 PostgreSQL 完全同构，因此直接继承官方 {@link PostgreSQLDelegate}：
 * <ul>
 *   <li>BLOB 列：使用 BYTEA + {@code setBytes/getBytes}（由 PostgreSQLDelegate 处理，
 *       GaussDB 同为 BYTEA 内核类型）</li>
 *   <li>集群锁：{@code SELECT ... FOR UPDATE} 行锁语义一致</li>
 *   <li>BOOLEAN 列绑定（IS_DURABLE 等）：JDBC setBoolean 双库一致</li>
 * </ul>
 *
 * <h3>GaussDB 差异抹平（由框架其他层完成，见 SchedulerDialect）</h3>
 * <ul>
 *   <li>空字符串写入退化为 NULL：框架 DAO 在写库前统一做"空串 → NULL"归一化，
 *       必填字段（task_name 等）强制非空校验，
 *       因此 Quartz 表中的 JOB_NAME / JOB_GROUP / SCHED_NAME 亦由框架保证非空</li>
 *   <li>{@code VARCHAR(n)} 按字节计数：建表脚本（deploy/sql/schema-gaussdb.sql）
 *       已将含中文可能的字段长度放大</li>
 *   <li>INSERT 回填（RETURNING/getGeneratedKeys）行为差异：业务表主键改走
 *       SEQUENCE nextval 预取（见 JdbcTaskRepository）</li>
 * </ul>
 *
 * <h3>预留扩展点</h3>
 * <p>如目标 GaussDB 版本对 boolean 字面量绑定或锁语法存在差异，
 * 在本类覆写 {@link PostgreSQLDelegate} 对应方法即可，无需改动框架其他代码；
 * 通过 {@code orbit.scheduler.database.dialect=gaussdb} 或自动探测启用本类。
 *
 * <p>注：Quartz 2.4+ 的 Delegate 采用无参构造 + 属性注入方式实例化，
 * 故本类仅声明无参构造。
 *
 * @author orbit
 */
public class GaussDBDelegate extends PostgreSQLDelegate {
}
