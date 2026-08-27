package com.orbit.scheduler.storage;

import com.orbit.scheduler.model.JobLog;
import com.orbit.scheduler.model.PageResult;
import com.orbit.scheduler.spi.JobLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存执行日志：环形队列（容量可配），适用于无数据库场景。
 *
 * <p><b>性能优化</b>：在环形 Deque 之外额外维护一个 {@link ConcurrentHashMap} 索引
 * {@code id -> JobLog}，将 {@link #appendFinish} 的查找复杂度从 O(N) 降到 O(1)；
 * 当日志因容量淘汰被移除时同步清理索引项，避免索引无限膨胀。
 *
 * @author orbit
 */
public class InMemoryJobLogRepository implements JobLogRepository {

    private static final Logger log = LoggerFactory.getLogger(InMemoryJobLogRepository.class);

    private final Deque<JobLog> deque = new ConcurrentLinkedDeque<JobLog>();
    /** id → JobLog 索引：供 appendFinish O(1) 定位 */
    private final ConcurrentHashMap<Long, JobLog> index = new ConcurrentHashMap<Long, JobLog>();
    private final AtomicLong idSeq = new AtomicLong(1);
    private final int capacity;

    public InMemoryJobLogRepository(int capacity) {
        this.capacity = capacity > 0 ? capacity : 1000;
    }

    @Override
    public Long appendStart(JobLog jobLog) {
        long id = idSeq.getAndIncrement();
        jobLog.setId(id);
        deque.addFirst(jobLog);
        index.put(id, jobLog);
        trim();
        return id;
    }

    @Override
    public void appendFinish(Long id, String status, String workerNode, long costMs, String message) {
        if (id == null) {
            return;
        }
        // 优先走索引 O(1)，索引未命中（日志已被淘汰）才降级遍历
        JobLog target = index.get(id);
        if (target == null) {
            target = scanDeque(id);
            if (target == null) {
                return;
            }
        }
        target.setStatus(status);
        target.setWorkerNode(workerNode);
        target.setCostMs(costMs);
        target.setEndTime(new java.util.Date());
        target.setMessage(abbreviate(message));
    }

    private JobLog scanDeque(Long id) {
        for (JobLog l : deque) {
            if (id.equals(l.getId())) {
                return l;
            }
        }
        return null;
    }

    @Override
    public PageResult<JobLog> page(String taskName, int page, int size) {
        // 任务名过滤后保留原顺序（最新在前）
        List<JobLog> filtered = new ArrayList<JobLog>();
        Iterator<JobLog> it = deque.iterator();
        while (it.hasNext()) {
            JobLog l = it.next();
            if (taskName == null || taskName.isEmpty() || taskName.equals(l.getTaskName())) {
                filtered.add(l);
            }
        }
        int total = filtered.size();
        int from = Math.max(0, (page - 1) * size);
        int to = Math.min(total, from + size);
        List<JobLog> items = from >= total ? new ArrayList<JobLog>() : new ArrayList<JobLog>(filtered.subList(from, to));
        return new PageResult<JobLog>(page, size, total, items);
    }

    @Override
    public String type() {
        return "memory";
    }

    private void trim() {
        while (deque.size() > capacity) {
            JobLog removed = deque.pollLast();
            if (removed == null) {
                return;
            }
            index.remove(removed.getId());
        }
    }

    static String abbreviate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 4000 ? message : message.substring(0, 4000) + "...(truncated)";
    }
}
