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
 * 扫描并注册所有 {@link OrbitJob} 方法。
 */
public class JobHandlerRegistry implements SmartInitializingSingleton, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(JobHandlerRegistry.class);

    private final Map<String, Handler> handlers = new ConcurrentHashMap<String, Handler>();
    private ApplicationContext ctx;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.ctx = applicationContext;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (ctx == null) {
            return;
        }
        for (String beanName : ctx.getBeanDefinitionNames()) {
            Class<?> type;
            try {
                type = ctx.getType(beanName);
            } catch (Exception e) {
                continue;
            }
            if (type == null || type.getName().startsWith("org.springframework.")) {
                continue;
            }
            Map<Method, OrbitJob> annotated = MethodIntrospector.selectMethods(type,
                    (MethodIntrospector.MetadataLookup<OrbitJob>) method ->
                            AnnotatedElementUtils.findMergedAnnotation(method, OrbitJob.class));
            if (annotated.isEmpty()) {
                continue;
            }
            Object bean = ctx.getBean(beanName);
            for (Map.Entry<Method, OrbitJob> e : annotated.entrySet()) {
                register(bean, type, e.getKey(), e.getValue());
            }
        }
        log.info("[orbit-executor] registered {} handler(s): {}", handlers.size(), handlers.keySet());
    }

    private void register(Object bean, Class<?> type, Method method, OrbitJob anno) {
        String name = anno.value();
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalStateException("@OrbitJob value empty on " + method);
        }
        name = name.trim();
        if (handlers.containsKey(name)) {
            throw new IllegalStateException("duplicate @OrbitJob handler: " + name);
        }
        for (Class<?> p : method.getParameterTypes()) {
            if (p != JobContext.class && p != Map.class && p != Object.class) {
                throw new IllegalStateException("@OrbitJob " + method + " unsupported param " + p.getName()
                        + "; allow: none / JobContext / Map");
            }
        }
        Method invocable = AopUtils.selectInvocableMethod(method, type);
        ReflectionUtils.makeAccessible(invocable);
        handlers.put(name, new Handler(bean, invocable, name));
        log.info("[orbit-executor] handler '{}' -> {}#{}", name, type.getSimpleName(), method.getName());
    }

    public boolean has(String handler) {
        return handlers.containsKey(handler);
    }

    public Object invoke(String handler, JobContext context) {
        Handler h = handlers.get(handler);
        if (h == null) {
            throw new IllegalArgumentException("handler not found: " + handler);
        }
        return h.invoke(context);
    }

    public List<String> listNames() {
        List<String> names = new ArrayList<String>(handlers.keySet());
        Collections.sort(names);
        return names;
    }

    static final class Handler {
        private final Object bean;
        private final Method method;
        private final String name;

        Handler(Object bean, Method method, String name) {
            this.bean = bean;
            this.method = method;
            this.name = name;
        }

        Object invoke(JobContext context) {
            Class<?>[] types = method.getParameterTypes();
            Object[] args = new Object[types.length];
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
                Throwable c = e.getCause() == null ? e : e.getCause();
                throw new RuntimeException("handler '" + name + "' failed: " + c.getMessage(), c);
            } catch (Exception e) {
                throw new RuntimeException("handler '" + name + "' invoke error: " + e.getMessage(), e);
            }
        }
    }
}
