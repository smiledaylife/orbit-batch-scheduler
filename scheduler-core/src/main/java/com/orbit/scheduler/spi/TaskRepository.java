package com.orbit.scheduler.spi;

import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.PageResult;

import java.util.List;
import java.util.Optional;

/**
 * 任务元数据存储 SPI：支持数据库存储（t_job_config）与本地内存存储两种实现。
 *
 * <p>存储模式由 {@code orbit.scheduler.storage.type} 决定：
 * <ul>
 *   <li>{@code database} —— 集群共享，多节点一致，动态增删改（生产推荐）</li>
 *   <li>{@code memory}   —— 本地存储，零依赖，重启即失（轻量场景/演示）</li>
 * </ul>
 *
 * @author orbit
 */
public interface TaskRepository {

    /** 查询全部任务配置 */
    List<JobConfig> findAll();

    /** 按任务名查询 */
    Optional<JobConfig> findByName(String taskName);

    /** 任务总数（仅用于健康检查/集群总览；不应在请求热点路径调用 findAll 后取 size） */
    default long count() {
        return findAll().size();
    }

    /** 新增或更新（带乐观锁校验），返回落库后的最新配置 */
    JobConfig save(JobConfig config);

    /** 删除 */
    boolean delete(String taskName);

    /** 分页查询（nameLike 为任务名模糊过滤，可空） */
    PageResult<JobConfig> page(String nameLike, int page, int size);

    /** 存储类型标识：database / memory */
    String type();
}
