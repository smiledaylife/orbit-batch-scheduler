package com.orbit.scheduler.storage;

import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.PageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 内存任务存储测试：CRUD / 乐观锁 / 分页。
 */
class InMemoryTaskRepositoryTest {

    private InMemoryTaskRepository repository;

    @BeforeEach
    void setUp() {
        repository = new InMemoryTaskRepository();
    }

    @Test
    void shouldSaveFindAndDelete() {
        JobConfig config = new JobConfig("demoTask", "ORBIT", "0 0/5 * * * ?", null);
        repository.save(config);

        Optional<JobConfig> found = repository.findByName("demoTask");
        assertTrue(found.isPresent());
        assertEquals(1, found.get().getVersion());
        assertEquals("0 0/5 * * * ?", found.get().getCronExpression());

        assertTrue(repository.delete("demoTask"));
        assertFalse(repository.findByName("demoTask").isPresent());
        assertFalse(repository.delete("demoTask"));
    }

    @Test
    void shouldBumpVersionOnUpdate() {
        JobConfig config = new JobConfig("demoTask", "ORBIT", "0 0/5 * * * ?", null);
        JobConfig saved = repository.save(config);

        saved.setCronExpression("0 0/10 * * * ?");
        JobConfig updated = repository.save(saved);
        assertEquals(2, updated.getVersion());

        // 陈旧版本更新应失败（乐观锁）
        JobConfig stale = new JobConfig("demoTask", "ORBIT", "0 0/15 * * * ?", null);
        stale.setVersion(1);
        assertThrows(IllegalStateException.class, () -> repository.save(stale));
    }

    @Test
    void shouldPageAndFilter() {
        for (int i = 1; i <= 5; i++) {
            repository.save(new JobConfig("task-" + i, "ORBIT", "0 0 " + i + " * * ?", null));
        }
        PageResult<JobConfig> all = repository.page(null, 1, 10);
        assertEquals(5, all.getTotal());

        PageResult<JobConfig> page2 = repository.page(null, 2, 2);
        assertEquals(5, page2.getTotal());
        assertEquals(2, page2.getItems().size());

        PageResult<JobConfig> filtered = repository.page("task-1", 1, 10);
        assertEquals(1, filtered.getTotal());
        assertEquals("task-1", filtered.getItems().get(0).getTaskName());
    }
}
