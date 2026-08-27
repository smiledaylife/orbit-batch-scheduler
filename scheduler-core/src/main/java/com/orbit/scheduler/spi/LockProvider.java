package com.orbit.scheduler.spi;

import java.time.Duration;

/**
 * 分布式锁 SPI：集群防重复调度的核心扩展点。
 *
 * <p>框架内置三种实现，按 {@code orbit.scheduler.lock.type} 装配：
 * <ul>
 *   <li>{@code RedissonLockProvider} —— 基于 Redis（Redisson，watchdog 自动续期）</li>
 *   <li>{@code JdbcLockProvider} —— 基于数据库行锁 + 过期时间戳（t_cluster_lock 表）</li>
 *   <li>{@code NoOpLockProvider} —— 无锁（单机/测试）</li>
 * </ul>
 *
 * <p>语义约定：
 * <ul>
 *   <li>tryLock 非阻塞：立即返回成败；锁非重入（同 owner 二次加锁同样失败）</li>
 *   <li>owner 用于安全释放与续期，防止误删他人锁</li>
 *   <li>lease 为租约时长；执行期间框架会周期性调用 renew 续期</li>
 *   <li>节点宕机后锁依赖租约超时自动释放（at-least-once 语义）</li>
 * </ul>
 *
 * @author orbit
 */
public interface LockProvider {

    /**
     * 尝试获取锁（非阻塞）。
     *
     * @param key   锁键（框架内部会加统一前缀）
     * @param owner 持有者标识（节点 ID）
     * @param lease 租约时长（超时自动失效）
     * @return true=获取成功
     */
    boolean tryLock(String key, String owner, Duration lease);

    /**
     * 续期锁（长任务运行期间由看门狗线程周期调用）。
     *
     * @return true=续期成功（仍由 owner 持有）
     */
    boolean renew(String key, String owner, Duration lease);

    /**
     * 释放锁（仅当 owner 匹配时生效，实现方需容忍锁已过期/已易主的情形）。
     */
    void unlock(String key, String owner);

    /** 实现类型标识：redis / database / none */
    String type();
}
