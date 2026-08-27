package com.orbit.scheduler.core;

import com.orbit.scheduler.annotation.DispatchType;
import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 工作流编排执行器测试：串行、失败中止、LOCAL 步骤。
 */
class WorkflowExecutorTest {

    /**
     * 使用可注入执行器的轻量 TaskRegistry 替身。
     */
    static class StubRegistry extends TaskRegistry {
        private final Map<String, java.util.concurrent.Callable<Object>> handlers =
                new HashMap<String, java.util.concurrent.Callable<Object>>();

        void put(String name, java.util.concurrent.Callable<Object> h) {
            handlers.put(name, h);
        }

        @Override
        public boolean hasTask(String taskName) {
            return handlers.containsKey(taskName);
        }

        @Override
        public Object execute(String taskName, TaskContext context) {
            try {
                return handlers.get(taskName).call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void sequentialLocalStepsSucceed() {
        StubRegistry stub = new StubRegistry();
        AtomicInteger c = new AtomicInteger();
        stub.put("stepA", () -> { c.incrementAndGet(); return "A"; });
        stub.put("stepB", () -> { c.incrementAndGet(); return "B"; });
        WorkflowExecutor ex = new WorkflowExecutor(stub, null, null, null, "n1");

        JobConfig cfg = workflowJob(false,
                localStep("s1", "stepA"),
                localStep("s2", "stepB"));

        WorkflowExecutor.WorkflowResult result = ex.execute(cfg, null, "req1", System.currentTimeMillis());
        assertTrue(result.isSuccess());
        assertEquals(2, result.getSteps().size());
        assertEquals(2, c.get());
        assertEquals("SUCCESS", result.getSteps().get(0).getStatus());
        assertEquals("SUCCESS", result.getSteps().get(1).getStatus());
    }

    @Test
    void failFastStopsSubsequentSteps() {
        StubRegistry stub = new StubRegistry();
        AtomicInteger c = new AtomicInteger();
        stub.put("stepA", () -> { c.incrementAndGet(); throw new IllegalStateException("fail-A"); });
        stub.put("stepB", () -> { c.incrementAndGet(); return "B"; });
        WorkflowExecutor ex = new WorkflowExecutor(stub, null, null, null, "n1");

        JobConfig cfg = workflowJob(true,
                localStep("s1", "stepA"),
                localStep("s2", "stepB"));

        WorkflowExecutor.WorkflowResult result = ex.execute(cfg, null, "req2", System.currentTimeMillis());
        assertFalse(result.isSuccess());
        assertEquals(2, result.getSteps().size());
        assertEquals("FAILED", result.getSteps().get(0).getStatus());
        assertEquals("SKIPPED", result.getSteps().get(1).getStatus());
        assertEquals(1, c.get(), "stepB must not run after failFast");
    }

    @Test
    void continueOnFailureRunsNext() {
        StubRegistry stub = new StubRegistry();
        AtomicInteger c = new AtomicInteger();
        stub.put("stepA", () -> { c.incrementAndGet(); throw new IllegalStateException("fail-A"); });
        stub.put("stepB", () -> { c.incrementAndGet(); return "B"; });
        WorkflowExecutor ex = new WorkflowExecutor(stub, null, null, null, "n1");

        WorkflowDefinition.Step s1 = localStep("s1", "stepA");
        s1.setContinueOnFailure(true);
        JobConfig cfg = workflowJob(true, s1, localStep("s2", "stepB"));

        WorkflowExecutor.WorkflowResult result = ex.execute(cfg, null, "req3", System.currentTimeMillis());
        assertFalse(result.isSuccess());
        assertEquals(2, c.get());
        assertEquals("SUCCESS", result.getSteps().get(1).getStatus());
    }

    private static JobConfig workflowJob(boolean failFast, WorkflowDefinition.Step... steps) {
        WorkflowDefinition def = new WorkflowDefinition();
        def.setMode("SEQUENTIAL");
        def.setFailFast(failFast);
        List<WorkflowDefinition.Step> list = new ArrayList<WorkflowDefinition.Step>();
        for (WorkflowDefinition.Step s : steps) {
            list.add(s);
        }
        def.setSteps(list);

        JobConfig cfg = new JobConfig();
        cfg.setTaskName("wf");
        cfg.setDispatchType(DispatchType.WORKFLOW);
        cfg.setTimeoutSeconds(30);
        try {
            cfg.setWorkflowDef(new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(def));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cfg;
    }

    private static WorkflowDefinition.Step localStep(String name, String taskName) {
        WorkflowDefinition.Step s = new WorkflowDefinition.Step();
        s.setName(name);
        s.setDispatchType(DispatchType.LOCAL);
        s.setTaskName(taskName);
        return s;
    }
}
