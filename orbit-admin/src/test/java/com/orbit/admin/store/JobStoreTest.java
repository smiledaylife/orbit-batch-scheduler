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
    void rejectUnserializableParams() {
        // 序列化失败必须快速失败并给出明确原因：若吞掉异常返回 null，
        // 任务参数会被静默清空入库 —— 接口返回 200、任务照常调度，
        // 但执行器拿到的是空参数，属于不可观测的故障。
        JobInfo job = newJob("badParams");
        java.util.Map<String, Object> params = new java.util.HashMap<String, Object>();
        // Jackson 默认 FAIL_ON_EMPTY_BEANS=true：没有任何可序列化属性的对象会抛异常
        params.put("bad", new Object());
        job.setParams(params);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> jobStore.saveJob(job));
        assertTrue(ex.getMessage().contains("params cannot be serialized"), ex.getMessage());
    }

    @Test
    void oversizedMessageIsTruncatedWithinColumnWidth() {
        // 截断后的总长度（含省略号）必须不超过 message VARCHAR(2000)，
        // 否则执行器返回长异常堆栈时 finishLog 会报「value too long」入库失败。
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

        java.util.List<String> reaped = jobStore.reapOrphanedRunning(3600 * 1000L, 600 * 1000L,
                java.util.Collections.<String>emptySet(), "hard cap", "executor offline");

        // 返回被回收的 logId：调用方要据此释放这些任务的串行守卫
        assertEquals(1, reaped.size());
        assertEquals("log-old", reaped.get(0));
        JobLog old = findByLogId("log-old");
        JobLog fresh = findByLogId("log-new");
        assertEquals("FAILED", old.getStatus());
        assertEquals("RUNNING", fresh.getStatus());
        assertNotNull(old.getEndTime());
    }

    @Test
    void reapKeepsRunningJobWhoseExecutorIsStillAlive() {
        // 执行器还在线、只是任务跑得久：不能判失败（对齐 XXL-JOB findLostJobIds 的 t2.id IS NULL 条件）
        java.util.Date old = new java.util.Date(System.currentTimeMillis() - 2 * 3600 * 1000L);
        insertRunningLog("log-alive", old, "http://10.0.0.1:8081");

        java.util.List<String> reaped = jobStore.reapOrphanedRunning(
                6 * 3600 * 1000L, 600 * 1000L,
                java.util.Collections.singleton("http://10.0.0.1:8081"), "hard cap", "executor offline");

        assertEquals(0, reaped.size());
        assertEquals("RUNNING", findByLogId("log-alive").getStatus());
        // 运行中的日志也应能看到承接节点
        assertEquals("http://10.0.0.1:8081", findByLogId("log-alive").getExecutorAddress());
    }

    @Test
    void reapFailsRunningJobWhoseExecutorWentOffline() {
        java.util.Date old = new java.util.Date(System.currentTimeMillis() - 2 * 3600 * 1000L);
        insertRunningLog("log-dead", old, "http://10.0.0.9:8081");

        java.util.List<String> reaped = jobStore.reapOrphanedRunning(
                6 * 3600 * 1000L, 600 * 1000L,
                java.util.Collections.singleton("http://10.0.0.1:8081"), "hard cap", "executor offline");

        assertEquals(1, reaped.size());
        JobLog gone = findByLogId("log-dead");
        assertEquals("FAILED", gone.getStatus());
        assertTrue(gone.getMessage().contains("offline"), "got: " + gone.getMessage());
    }

    @Test
    void reapHardCapAppliesEvenWhenExecutorIsAlive() {
        // 硬上界兜底：执行器活着但结果永远回不来时，不能留下永久 RUNNING 的日志
        java.util.Date old = new java.util.Date(System.currentTimeMillis() - 5 * 3600 * 1000L);
        insertRunningLog("log-stuck", old, "http://10.0.0.1:8081");

        java.util.List<String> reaped = jobStore.reapOrphanedRunning(
                3600 * 1000L, 600 * 1000L,
                java.util.Collections.singleton("http://10.0.0.1:8081"), "hard cap", "executor offline");

        assertEquals(1, reaped.size());
        JobLog stuck = findByLogId("log-stuck");
        assertEquals("FAILED", stuck.getStatus());
        assertTrue(stuck.getMessage().contains("hard cap"), "got: " + stuck.getMessage());
    }

    @Test
    void finishLogFromRunningOnlyConvergesRunningLogs() {
        insertRunningLog("log-cb", new java.util.Date());

        // 首次回传：RUNNING -> SUCCESS
        assertTrue(jobStore.finishLogFromRunning("log-cb", true, "http://10.0.0.1:8081", 123L, "done"));
        assertEquals("SUCCESS", findByLogId("log-cb").getStatus());
        assertEquals(123L, findByLogId("log-cb").getCostMs());

        // 重复回传（执行器重试）：已经不是 RUNNING，必须被忽略，不能覆盖真实结果
        assertEquals(false, jobStore.finishLogFromRunning("log-cb", false, "http://10.0.0.2:8081", 999L, "late fail"));
        JobLog after = findByLogId("log-cb");
        assertEquals("SUCCESS", after.getStatus());
        assertEquals(123L, after.getCostMs());
    }

    @Test
    void finishLogFromRunningIgnoresUnknownLogId() {
        assertEquals(false, jobStore.finishLogFromRunning("log-absent", true, "n", 1L, "x"));
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
        insertRunningLog(logId, startTime, null);
    }

    private void insertRunningLog(String logId, java.util.Date startTime, String executorAddress) {
        JobLog running = new JobLog();
        running.setLogId(logId);
        running.setJobName("jobLog");
        running.setAppName("demo-executor");
        running.setHandler("dailyReport");
        running.setStatus("RUNNING");
        running.setStartTime(startTime);
        jobStore.insertLog(running);
        if (executorAddress != null) {
            // 模拟 dispatch 受理时写入的承接节点
            jobStore.markDispatched(logId, executorAddress);
        }
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
