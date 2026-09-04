package com.orbit.executor;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link JobContext} 类型化取参测试：
 * 验证 getString / getInt / getLong / getDouble / getBoolean 对
 * JSON 数字、字符串数字、布尔与非法值的解析与默认值回退行为。
 */
class JobContextTest {

    private static JobContext ctx(Map<String, Object> params) {
        return new JobContext(1L, "job", "handler", "log-1", params);
    }

    @Test
    void stringAccessors() {
        Map<String, Object> p = new HashMap<String, Object>();
        p.put("name", "orbit");
        p.put("num", 42);
        JobContext c = ctx(p);

        assertEquals("orbit", c.getString("name"));
        assertEquals("42", c.getString("num"));
        assertEquals(null, c.getString("missing"));
        assertEquals("def", c.getString("missing", "def"));
    }

    @Test
    void intAccessors() {
        Map<String, Object> p = new HashMap<String, Object>();
        p.put("n", 42);
        p.put("s", "123");
        p.put("bad", "xyz");
        JobContext c = ctx(p);

        assertEquals(42, c.getInt("n", -1));
        assertEquals(123, c.getInt("s", -1));
        assertEquals(-1, c.getInt("bad", -1));
        assertEquals(-1, c.getInt("missing", -1));
    }

    @Test
    void longAccessors() {
        Map<String, Object> p = new HashMap<String, Object>();
        p.put("big", 9876543210L);
        p.put("s", "9876543210");
        JobContext c = ctx(p);

        assertEquals(9876543210L, c.getLong("big", -1L));
        assertEquals(9876543210L, c.getLong("s", -1L));
        assertEquals(-1L, c.getLong("missing", -1L));
    }

    @Test
    void doubleAccessors() {
        Map<String, Object> p = new HashMap<String, Object>();
        p.put("d", 3.14);
        p.put("s", "2.71");
        JobContext c = ctx(p);

        assertEquals(3.14, c.getDouble("d", -1), 1e-9);
        assertEquals(2.71, c.getDouble("s", -1), 1e-9);
        assertEquals(-1, c.getDouble("missing", -1), 1e-9);
    }

    @Test
    void booleanAccessors() {
        Map<String, Object> p = new HashMap<String, Object>();
        p.put("t", Boolean.TRUE);
        p.put("f", "false");
        p.put("bad", "nope");
        JobContext c = ctx(p);

        assertEquals(true, c.getBoolean("t", false));
        assertEquals(false, c.getBoolean("f", true));
        // 非法值回退默认值而非抛异常
        assertEquals(true, c.getBoolean("bad", true));
        assertEquals(false, c.getBoolean("missing", false));
    }

    @Test
    void nullParamsAreSafe() {
        JobContext c = ctx(null);
        assertEquals(0, c.getParams().size());
        assertEquals("def", c.getString("any", "def"));
        assertEquals(7, c.getInt("any", 7));
    }
}
