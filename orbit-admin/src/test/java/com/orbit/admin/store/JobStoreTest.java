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

    @Test
    void oversizedMessageIsTruncatedWithinColumnWidth() {
        // 回归测试：旧实现截断后拼接 "..." 会产生 2003 字符，超出 message VARCHAR(2000)，
        // 执行器返回长异常堆栈时 finishLog 直接报「value too long」入库失败。
        JobInfo job = jobStore.saveJob(newJob("bigMsg"));
        JobLog running = new JobLog();
        running.setLogId("log-big");
        running.setJobId(job.getId());
        running.setJobName("bigMsg");
        running.setStatus("RUNNING");
        running.setStartTime(new java.util.Date());
        jobStore.insertLog(running);

        String huge = repeat('e', 3000);
        jobStore.finishLog("log-big", "FAILED", null, 1L, huge);

        JobLog done = jobStore.pageLogs("bigMsg", 1, 10).getItems().get(0);
        assertNotNull(done.getMessage());
        assertTrue(done.getMessage().length() <= 2000, "message must fit column width: "
                + done.getMessage().length());
        assertTrue(done.getMessage().endsWith("..."));
    }

    @Test
    void reapOrphanedRunningLogs() {
        // 一条超龄 RUNNING（模拟 admin 崩溃遗留）+ 一条新 RUNNING（模拟正常执行中，不应被误杀）
        insertRunningLog("log-old", new java.util.Date(System.currentTimeMillis() - 2 * 3600 * 1000L));
        insertRunningLog("log-new", new java.util.Date());

        int reaped = jobStore.reapOrphanedRunning(3600 * 1000L, "orphaned running log");

        assertEquals(1, reaped);
        JobLog old = findByLogId("log-old");
        JobLog fresh = findByLogId("log-new");
        assertEquals("FAILED", old.getStatus());
        assertEquals("RUNNING", fresh.getStatus());
        assertNotNull(old.getEndTime());
    }

    @Test
    void deleteLogsBeforeRespectsCutoff() {
        long now = System.currentTimeMillis();
        // 三条超期日志 + 一条保留期内日志
        insertFinishedLog("log-d1", new java.util.Date(now - 40L * 24 * 3600 * 1000));
        insertFinishedLog("log-d2", new java.util.Date(now - 35L * 24 * 3600 * 1000));
        insertFinishedLog("log-d3", new java.util.Date(now - 31L * 24 * 3600 * 1000));
        insertFinishedLog("log-keep", new java.util.Date(now - 1L * 24 * 3600 * 1000));

        java.util.Date cutoff = new java.util.Date(now - 30L * 24 * 3600 * 1000);
        int deleted = jobStore.deleteLogsBefore(cutoff);

        assertEquals(3, deleted);
        assertEquals(1, jobStore.pageLogs(null, 1, 10).getTotal());
        assertNotNull(findByLogId("log-keep"));
    }

    private void insertRunningLog(String logId, java.util.Date startTime) {
        JobLog running = new JobLog();
        running.setLogId(logId);
        running.setJobName("jobLog");
        running.setAppName("demo-executor");
        running.setHandler("dailyReport");
        running.setStatus("RUNNING");
        running.setStartTime(startTime);
        jobStore.insertLog(running);
    }

    private void insertFinishedLog(String logId, java.util.Date startTime) {
        JobLog finished = new JobLog();
        finished.setLogId(logId);
        finished.setJobName("jobLog");
        finished.setAppName("demo-executor");
        finished.setHandler("dailyReport");
        finished.setStatus("SUCCESS");
        finished.setStartTime(startTime);
        finished.setEndTime(new java.util.Date(startTime.getTime() + 1000));
        finished.setCostMs(1000L);
        jobStore.insertLog(finished);
    }

    private JobLog findByLogId(String logId) {
        for (JobLog l : jobStore.pageLogs("jobLog", 1, 200).getItems()) {
            if (logId.equals(l.getLogId())) {
                return l;
            }
        }
        return null;
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}
