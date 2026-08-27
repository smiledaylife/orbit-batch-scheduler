package com.orbit.scheduler.storage;

import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.PageResult;
import com.orbit.scheduler.spi.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地内存任务存储：零依赖、重启即失。
 *
 * <p>适用于单机轻量场景，或与 Quartz 内存 JobStore 搭配的 standalone 模式。
 * 集群部署（多副本）时请使用 {@link JdbcTaskRepository}。
 *
 * <p><b>性能优化</b>：使用 {@link LinkedHashMap} 之外额外的快速 count 路径，
 * 避免每次 overview 都构造临时列表。
 *
 * @author orbit
 */
public class InMemoryTaskRepository implements TaskRepository {

    private static final Logger log = LoggerFactory.getLogger(InMemoryTaskRepository.class);

    private final Map<String, JobConfig> store = new LinkedHashMap<String, JobConfig>();
    private final AtomicLong idSeq = new AtomicLong(1);

    @Override
    public synchronized List<JobConfig> findAll() {
        List<JobConfig> list = new ArrayList<JobConfig>(store.values());
        list.sort(Comparator.comparing(JobConfig::getTaskName,
                Comparator.nullsLast(String::compareTo)));
        return list;
    }

    @Override
    public synchronized Optional<JobConfig> findByName(String taskName) {
        return Optional.ofNullable(store.get(taskName));
    }

    @Override
    public synchronized long count() {
        return store.size();
    }

    @Override
    public synchronized JobConfig save(JobConfig config) {
        JobConfig existing = store.get(config.getTaskName());
        Date now = new Date();
        if (existing == null) {
            config.setId(idSeq.getAndIncrement());
            config.setVersion(1);
            config.setCreatedAt(now);
            config.setUpdatedAt(now);
            store.put(config.getTaskName(), config);
            log.info("[orbit-scheduler] memory-saved new task config: {}", config.getTaskName());
            return config;
        }
        if (config.getVersion() != existing.getVersion()) {
            throw new IllegalStateException("Task '" + config.getTaskName()
                    + "' was modified concurrently, expected version " + existing.getVersion()
                    + " but got " + config.getVersion() + ", please retry");
        }
        existing.copyEditableFrom(config);
        existing.setVersion(existing.getVersion() + 1);
        existing.setUpdatedAt(now);
        return existing;
    }

    @Override
    public synchronized boolean delete(String taskName) {
        return store.remove(taskName) != null;
    }

    @Override
    public synchronized PageResult<JobConfig> page(String nameLike, int page, int size) {
        List<JobConfig> all = new ArrayList<JobConfig>();
        for (JobConfig c : store.values()) {
            if (nameLike == null || nameLike.isEmpty()
                    || (c.getTaskName() != null && c.getTaskName().contains(nameLike))) {
                all.add(c);
            }
        }
        all.sort(Comparator.comparing(JobConfig::getTaskName,
                Comparator.nullsLast(String::compareTo)));
        int total = all.size();
        int from = Math.max(0, (page - 1) * size);
        int to = Math.min(total, from + size);
        List<JobConfig> items = from >= total ? new ArrayList<JobConfig>() : all.subList(from, to);
        return new PageResult<JobConfig>(page, size, total, new ArrayList<JobConfig>(items));
    }

    @Override
    public String type() {
        return "memory";
    }
}
