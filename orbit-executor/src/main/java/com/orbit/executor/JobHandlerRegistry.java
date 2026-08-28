package com.orbit.executor;

import com.orbit.executor.annotation.OrbitJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务处理函数（JobHandler）注册表与反射调用分发器。
 * <p>核心机制：
 * <ul>
 *   <li>实现 {@link SmartInitializingSingleton} 接口，在 Spring 容器单例 Bean 实例化完成后，扫描所有带有 {@link OrbitJob} 注解的方法；</li>
 *   <li>校验方法参数合法性（仅允许无参、{@link JobContext} 或 {@link Map}）；</li>
 *   <li>解决 Spring CGLIB/JDK 动态代理问题，提取真实可调用的底层 Method；</li>
 *   <li>维护本地 Handler 注册缓存，响应调度中心触发请求时通过反射快速调用业务逻辑。</li>
 * </ul>
 */
public class JobHandlerRegistry implements SmartInitializingSingleton, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(JobHandlerRegistry.class);

    /**
     * 本地 JobHandler 缓存映射表（key: Handler 唯一名称，value: 封装的方法反射执行器）
     */
    private final Map<String, Handler> handlers = new ConcurrentHashMap<String, Handler>();

    /**
     * Spring 容器应用上下文
     */
    private ApplicationContext ctx;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.ctx = applicationContext;
    }

    /**
     * 在所有单例 Bean 实例化完成后触发，自动扫描 Spring 容器中所有的 @OrbitJob 注解方法
     */
    @Override
    public void afterSingletonsInstantiated() {
        if (ctx == null) {
            return;
        }

        // 遍历 Spring 容器中定义的所有 Bean 名称
        for (String beanName : ctx.getBeanDefinitionNames()) {
            Class<?> type;
            try {
                type = ctx.getType(beanName);
            } catch (Exception e) {
                continue;
            }

            // 过滤 Spring 框架内部自身的底层 Bean
            if (type == null || type.getName().startsWith("org.springframework.")) {
                continue;
            }

            // 内省检索当前 Bean 包含 @OrbitJob 注解的所有方法（兼容 CGLIB 代理和组合注解）
            Map<Method, OrbitJob> annotated = MethodIntrospector.selectMethods(type,
                    (MethodIntrospector.MetadataLookup<OrbitJob>) method ->
                            AnnotatedElementUtils.findMergedAnnotation(method, OrbitJob.class));

            if (annotated.isEmpty()) {
                continue;
            }

            // 获取 Bean 实例并逐个方法注册到缓存中
            Object bean = ctx.getBean(beanName);
            for (Map.Entry<Method, OrbitJob> e : annotated.entrySet()) {
                register(bean, type, e.getKey(), e.getValue());
            }
        }
        log.info("[orbit-executor] registered {} handler(s): {}", handlers.size(), handlers.keySet());
    }

    /**
     * 注册单个 @OrbitJob 注解方法，校验参数类型并将其存入缓存
     *
     * @param bean   Spring 托管的 Bean 实例
     * @param type   Bean 的 Class 类型
     * @param method 带有注解的方法反射对象
     * @param anno   @OrbitJob 注解元信息
     */
    private void register(Object bean, Class<?> type, Method method, OrbitJob anno) {
        String name = anno.value();
        // 校验 Handler 标识不可为空
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalStateException("@OrbitJob value empty on " + method);
        }
        name = name.trim();

        // 避免不同 Bean 之间定义了重复的 Handler 名称
        if (handlers.containsKey(name)) {
            throw new IllegalStateException("duplicate @OrbitJob handler: " + name);
        }

        // 参数合法性严格校验：支持无参、JobContext 或 Map 参数
        for (Class<?> p : method.getParameterTypes()) {
            if (p != JobContext.class && p != Map.class && p != Object.class) {
                throw new IllegalStateException("@OrbitJob " + method + " unsupported param " + p.getName()
                        + "; allow: none / JobContext / Map");
            }
        }

        // 处理 AOP 代理场景，获取原始可调用的目标方法，并设置访问权限
        Method invocable = AopUtils.selectInvocableMethod(method, type);
        ReflectionUtils.makeAccessible(invocable);

        // 存入内存注册表
        handlers.put(name, new Handler(bean, invocable, name));
        log.info("[orbit-executor] handler '{}' -> {}#{}", name, type.getSimpleName(), method.getName());
    }

    /**
     * 判断当前执行器是否存在指定名称的 Handler
     *
     * @param handler Handler 名称
     * @return true 表示存在，false 表示不存在
     */
    public boolean has(String handler) {
        return handlers.containsKey(handler);
    }

    /**
     * 根据 Handler 名称执行对应的业务逻辑函数
     *
     * @param handler Handler 唯一名称
     * @param context 任务执行上下文
     * @return 业务方法返回值
     */
    public Object invoke(String handler, JobContext context) {
        Handler h = handlers.get(handler);
        if (h == null) {
            throw new IllegalArgumentException("handler not found: " + handler);
        }
        return h.invoke(context);
    }

    /**
     * 获取当前执行器注册的所有 Handler 名称列表（升序排列）
     *
     * @return Handler 名称列表
     */
    public List<String> listNames() {
        List<String> names = new ArrayList<String>(handlers.keySet());
        Collections.sort(names);
        return names;
    }

    /**
     * 封装业务 Bean 与反射执行 Method 的内部执行器
     */
    static final class Handler {
        /**
         * 业务 Bean 实例
         */
        private final Object bean;

        /**
         * 目标反射执行方法
         */
        private final Method method;

        /**
         * Handler 注册名称
         */
        private final String name;

        Handler(Object bean, Method method, String name) {
            this.bean = bean;
            this.method = method;
            this.name = name;
        }

        /**
         * 反射执行方法，并根据参数类型完成上下文参数注入
         *
         * @param context 任务执行上下文
         * @return 方法执行结果
         */
        Object invoke(JobContext context) {
            Class<?>[] types = method.getParameterTypes();
            Object[] args = new Object[types.length];
            // 动态匹配入参类型：若为 JobContext 则注入上下文，否则注入 params Map
            for (int i = 0; i < types.length; i++) {
                if (types[i] == JobContext.class) {
                    args[i] = context;
                } else {
                    args[i] = context.getParams();
                }
            }
            try {
                return method.invoke(bean, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                // 解包 InvocationTargetException，抛出真实的底层业务异常原因
                Throwable c = e.getCause() == null ? e : e.getCause();
                throw new RuntimeException("handler '" + name + "' failed: " + c.getMessage(), c);
            } catch (Exception e) {
                throw new RuntimeException("handler '" + name + "' invoke error: " + e.getMessage(), e);
            }
        }
    }
}
