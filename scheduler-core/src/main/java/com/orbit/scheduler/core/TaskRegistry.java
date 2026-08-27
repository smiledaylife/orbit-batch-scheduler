package com.orbit.scheduler.core;

import com.orbit.scheduler.annotation.BatchTask;
import com.orbit.scheduler.annotation.DispatchType;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务注册器：启动时扫描 Spring 容器中所有 {@link BatchTask} 注解方法，
 * 构建反射执行器，供本地调度 / HTTP 远程执行共同使用。
 *
 * <p>方法签名约束（启动期校验，违规即快速失败）：
 * 参数仅允许 TaskContext、Map&lt;String,Object&gt; 或无参，可任意组合。
 *
 * @author orbit
 */
public class TaskRegistry implements SmartInitializingSingleton, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(TaskRegistry.class);

    private final Map<String, TaskInvoker> tasks = new ConcurrentHashMap<String, TaskInvoker>();
    private volatile ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterSingletonsInstantiated() {
        scanAndRegister();
    }

    private void scanAndRegister() {
        ApplicationContext context = this.applicationContext;
        if (context == null) {
            return;
        }
        String[] beanNames = context.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            Class<?> beanType;
            try {
                beanType = context.getType(beanName);
            } catch (Exception e) {
                continue;
            }
            if (beanType == null || isInfrastructureClass(beanType)) {
                continue;
            }
            Map<Method, BatchTask> annotated = MethodIntrospector.selectMethods(beanType,
                    (MethodIntrospector.MetadataLookup<BatchTask>) method ->
                            AnnotatedElementUtils.findMergedAnnotation(method, BatchTask.class));
            if (annotated.isEmpty()) {
                continue;
            }
            for (Map.Entry<Method, BatchTask> entry : annotated.entrySet()) {
                registerBeanMethods(beanName, beanType, entry.getKey(), entry.getValue());
            }
        }
        log.info("[orbit-scheduler] TaskRegistry initialized, {} batch task(s) registered: {}",
                tasks.size(), tasks.keySet());
    }

    private void registerBeanMethods(String beanName, Class<?> beanType, Method method, BatchTask anno) {
        String taskName = anno.name();
        if (taskName == null || taskName.trim().isEmpty()) {
            throw new IllegalStateException("@BatchTask name must not be empty on method: " + method);
        }
        if (tasks.containsKey(taskName)) {
            throw new IllegalStateException("Duplicate @BatchTask name '" + taskName + "' on " + method);
        }
        validateParameters(method);

        Method invocableMethod;
        try {
            invocableMethod = AopUtils.selectInvocableMethod(method, beanType);
        } catch (IllegalStateException e) {
            throw new IllegalStateException("@BatchTask method cannot be invoked through proxy: " + method, e);
        }
        ReflectionUtils.makeAccessible(invocableMethod);
        Object bean = applicationContext.getBean(beanName);

        TaskInvoker invoker = new TaskInvoker(bean, invocableMethod);
        tasks.put(taskName, invoker);
        log.info("[orbit-scheduler] registered batch task '{}' -> {}#{} (dispatchType={})",
                taskName, beanType.getSimpleName(), method.getName(), anno.dispatchType());
    }

    private void validateParameters(Method method) {
        for (Class<?> type : method.getParameterTypes()) {
            if (type != TaskContext.class
                    && !(type == Map.class)
                    && type != Object.class) {
                throw new IllegalStateException(String.format(
                        "@BatchTask method %s declares unsupported parameter type %s. " +
                                "Allowed: none / TaskContext / Map<String,Object> / Object",
                        method, type.getName()));
            }
        }
    }

    private boolean isInfrastructureClass(Class<?> type) {
        String name = type.getName();
        return name.startsWith("org.springframework.") && !name.contains("$");
    }

    /** 是否存在指定任务的本地执行器 */
    public boolean hasTask(String taskName) {
        return tasks.containsKey(taskName);
    }

    /** 本地执行任务，返回方法返回值（可为 null） */
    public Object execute(String taskName, TaskContext context) {
        TaskInvoker invoker = tasks.get(taskName);
        if (invoker == null) {
            throw new TaskNotRegisteredException(taskName);
        }
        return invoker.invoke(context);
    }

    /** 全部已注册任务的元信息（按名称排序） */
    public List<TaskDefinition> getTaskDefinitions() {
        Map<String, TaskInvoker> snapshot = new LinkedHashMap<String, TaskInvoker>(tasks);
        List<String> names = new ArrayList<String>(snapshot.keySet());
        Collections.sort(names);
        List<TaskDefinition> result = new ArrayList<TaskDefinition>(names.size());
        for (String name : names) {
            result.add(snapshot.get(name).definition);
        }
        return result;
    }

    /** 本地执行器缺失时抛出 */
    public static class TaskNotRegisteredException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public TaskNotRegisteredException(String taskName) {
            super("Task '" + taskName + "' has no executor on this node");
        }
    }

    /** 反射执行器 + 元信息持有 */
    final class TaskInvoker {

        private final Object bean;
        private final Method method;
        private final TaskDefinition definition;

        TaskInvoker(Object bean, Method method) {
            this.bean = bean;
            this.method = method;
            BatchTask anno = method.getAnnotation(BatchTask.class);
            this.definition = new TaskDefinition(
                    anno.name(), anno.description(), anno.cron(), anno.dispatchType(),
                    anno.overwrite(), bean.getClass().getSimpleName(), method.getName());
        }

        Object invoke(TaskContext context) {
            Class<?>[] paramTypes = method.getParameterTypes();
            Object[] args = new Object[paramTypes.length];
            for (int i = 0; i < paramTypes.length; i++) {
                if (paramTypes[i] == TaskContext.class) {
                    args[i] = context;
                } else {
                    args[i] = context.getParams();
                }
            }
            try {
                return method.invoke(bean, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                throw new RuntimeException("Task '" + definition.getName()
                        + "' execution failed: " + cause.getMessage(), cause);
            } catch (Exception e) {
                throw new RuntimeException("Task '" + definition.getName() + "' invocation failed: " + e.getMessage(), e);
            }
        }
    }
}
