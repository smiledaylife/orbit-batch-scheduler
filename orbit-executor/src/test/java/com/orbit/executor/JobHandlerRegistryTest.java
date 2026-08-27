package com.orbit.executor;

import com.orbit.executor.annotation.OrbitJob;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobHandlerRegistryTest {

    @Component
    static class Sample {
        @OrbitJob("hello")
        public String hello(JobContext ctx) {
            return "hi-" + ctx.getString("name", "x");
        }

        @OrbitJob("mapJob")
        public String mapJob(Map<String, Object> params) {
            return String.valueOf(params.get("k"));
        }
    }

    @Test
    void scanAndInvoke() {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.register(Sample.class, JobHandlerRegistry.class);
        ctx.refresh();

        JobHandlerRegistry registry = ctx.getBean(JobHandlerRegistry.class);
        assertTrue(registry.has("hello"));
        assertTrue(registry.has("mapJob"));
        assertEquals(2, registry.listNames().size());

        JobContext jc = new JobContext(1, "j", "hello", "log1",
                java.util.Collections.<String, Object>singletonMap("name", "orbit"));
        assertEquals("hi-orbit", registry.invoke("hello", jc));

        JobContext jc2 = new JobContext(1, "j", "mapJob", "log2",
                java.util.Collections.<String, Object>singletonMap("k", "v"));
        assertEquals("v", registry.invoke("mapJob", jc2));
        ctx.close();
    }
}
