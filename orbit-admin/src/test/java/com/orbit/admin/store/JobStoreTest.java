package com.orbit.admin.store;

import com.baomidou.mybatisplus.test.autoconfigure.MybatisPlusTest;
import com.orbit.admin.config.MybatisPlusConfig;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.JobLog;
import com.orbit.core.model.PageResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.jdbc.Sql;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link JobStore} 持久层切片测试（MyBatis-Plus + 内存 H2 + Druid）。
 * 验证：任务 CRUD、分页、JSON 参数存取、乐观锁并发冲突、日志插入/完成、长度校验。
 * {@code @MybatisPlusTest} 默认事务回滚，测试间互不污染。
 */
@MybatisPlusTest
// 不替换为默认嵌入式库，沿用 test/resources/application.yml 中 PostgreSQL 兼容模式的 H2（schema.sql 使用 BIGSERIAL）
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({MybatisPlusConfig.class, JobStore.class})
@Sql(scripts = "/schema.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
class JobStoreTest {

    @Autowired
    private JobStore jobStore;

    /** 构造一个任务对象 */
    private static JobInfo newJob(String name) {
        JobInfo job = new JobInfo();
        job.setJobName(name);
        job.setDescription("desc-" + name);
        job.setAppName("demo-executor");
        job.setHandler("dailyReport");
        job.setCron("0 */2 * * * ?");
        job.setParams(Collections.<String, Object>singletonMap("bizDate", "yesterday"));
        job.setRouteStrategy("ROUND");
        job.setTimeoutSeconds(120);
        job.setEnabled(true);
        return job;
    }

    @Test
    void insertAndFind() {
        JobInfo saved = jobStore.saveJob(newJob("jobA"));
        assertNotNull(saved.getId());
        assertEquals(1, saved.getVersion());

        JobInfo found = jobStore.findJobByName("jobA").orElse(null);
        assertNotNull(found);
        assertEquals("dailyReport", found.getHandler());
        assertEquals(120, found.getTimeoutSeconds());
        assertTrue(found.isEnabled());
        // JSON 参数往返一致
        assertEquals("yesterday", found.getParams().get("bizDate"));
        assertEquals(1, jobStore.countJobs());
        assertTrue(jobStore.findJobById(saved.getId()).isPresent());
    }

    @Test
    void updateOptimisticLock() {
        JobInfo saved = jobStore.saveJob(newJob("jobB"));
        int version = saved.getVersion();

        saved.setDescription("updated");
        JobInfo updated = jobStore.saveJob(saved);
        assertEquals(version + 1, updated.getVersion());
        assertEquals("updated", jobStore.findJobByName("jobB").get().getDescription());

        // 用过期版本号更新 -> 乐观锁冲突
        JobInfo stale = jobStore.findJobByName("jobB").get();
        stale.setVersion(version); // 故意回退到旧版本
        stale.setDescription("stale write");
        assertThrows(IllegalStateException.class, () -> jobStore.saveJob(stale));
    }

    @Test
    void pageJobsWithNameLike() {
        jobStore.saveJob(newJob("abcReport"));
        jobStore.saveJob(newJob("abcSync"));
        jobStore.saveJob(newJob("zzz"));

        PageResult<JobInfo> all = jobStore.pageJobs(null, 1, 10);
        assertEquals(3, all.getTotal());

        PageResult<JobInfo> filtered = jobStore.pageJobs("abc", 1, 10);
        assertEquals(2, filtered.getTotal());
        assertEquals(2, filtered.getItems().size());

        // 分页 size 上限保护：传入超大 size 被收敛到 200
        PageResult<JobInfo> capped = jobStore.pageJobs(null, 1, 999);
        assertEquals(200, capped.getSize());
        assertEquals(3, capped.getTotal());
    }

    @Test
    void deleteJob() {
        jobStore.saveJob(newJob("jobDel"));
        assertTrue(jobStore.deleteJob("jobDel"));
        assertFalse(jobStore.findJobByName("jobDel").isPresent());
    }

    @Test
    void insertAndFinishLog() {
        JobInfo job = jobStore.saveJob(newJob("jobLog"));

        JobLog running = new JobLog();
        running.setLogId("log-1");
        running.setJobId(job.getId());
        running.setJobName("jobLog");
        running.setAppName("demo-executor");
        running.setHandler("dailyReport");
        running.setStatus("RUNNING");
        running.setStartTime(new java.util.Date());
        jobStore.insertLog(running);
        assertNotNull(running.getId());

        jobStore.finishLog("log-1", "SUCCESS", "http://10.0.0.1:8081", 123L, "ok");

        PageResult<JobLog> page = jobStore.pageLogs("jobLog", 1, 10);
        assertEquals(1, page.getTotal());
        JobLog done = page.getItems().get(0);
        assertEquals("SUCCESS", done.getStatus());
        assertEquals("http://10.0.0.1:8081", done.getExecutorAddress());
        assertEquals(123L, done.getCostMs());
        assertNotNull(done.getEndTime());
    }

    @Test
    void rejectOversizedParams() {
        JobInfo job = newJob("bigParams");
        // 构造超过 2000 列宽的 JSON
        java.util.Map<String, Object> big = new java.util.HashMap<String, Object>();
        big.put("payload", repeat('x', 3000));
        job.setParams(big);
        assertThrows(IllegalArgumentException.class, () -> jobStore.saveJob(job));
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}
