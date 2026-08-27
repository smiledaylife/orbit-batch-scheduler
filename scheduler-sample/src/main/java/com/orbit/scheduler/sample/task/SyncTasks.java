package com.orbit.scheduler.sample.task;

import com.orbit.scheduler.annotation.BatchTask;
import com.orbit.scheduler.annotation.DispatchType;
import com.orbit.scheduler.core.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 数据同步类任务示例：演示 HTTP 远程派发（经 K8s Service 路由到任意 Pod 执行）。
 *
 * <p>remoteDataSync 声明为 HTTP 派发：Quartz 触发节点不执行业务逻辑，
 * 而是将请求转发到 Headless Service 解析出的任一 Pod —— 在多副本部署中，
 * 执行日志里可以看到 dispatchNode 与 workerNode 是不同节点。
 *
 * @author orbit
 */
@Component
public class SyncTasks {

    private static final Logger log = LoggerFactory.getLogger(SyncTasks.class);

    /**
     * 跨节点数据同步：每 5 分钟，HTTP 派发演示。
     */
    @BatchTask(name = "remoteDataSync", description = "跨节点数据同步（HTTP派发演示）",
            cron = "0 */5 * * * ?", dispatchType = DispatchType.HTTP)
    public String remoteDataSync(TaskContext ctx) {
        log.info("[demo] remoteDataSync 在节点 {} 执行（由节点 {} 派发）, requestId={}",
                ctx.getWorkerNode(), ctx.getDispatchNode(), ctx.getRequestId());
        try {
            Thread.sleep(1200L + ThreadLocalRandom.current().nextInt(800));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "数据同步完成: worker=" + ctx.getWorkerNode() + ", requestId=" + ctx.getRequestId();
    }

    /**
     * 模拟失败任务：验证 FAILED 日志与异常堆栈记录。
     */
    @BatchTask(name = "failureDemo", description = "失败演示任务（随机 50% 抛异常）", cron = "0 */10 * * * ?")
    public String failureDemo(TaskContext ctx) {
        if (ThreadLocalRandom.current().nextInt(2) == 0) {
            throw new IllegalStateException("模拟业务异常: 下游接口超时");
        }
        return "本次执行成功";
    }
}
