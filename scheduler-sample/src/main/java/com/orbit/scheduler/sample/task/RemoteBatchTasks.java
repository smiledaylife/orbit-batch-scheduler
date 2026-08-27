package com.orbit.scheduler.sample.task;

import com.orbit.scheduler.annotation.BatchTask;
import com.orbit.scheduler.annotation.DispatchType;
import org.springframework.stereotype.Component;

/**
 * 跨服务批量调度示例。
 *
 * <p>REMOTE 类型任务的业务逻辑在<strong>其他微服务</strong>上，本类方法体不会被执行，
 * 仅作为注解种子（任务名 / cron / 目标服务 / 路径）写入任务存储，由调度框架定时
 * HTTP 调用目标服务接口。
 *
 * <p>目标服务只需暴露普通 REST 接口，例如：
 * <pre>
 * POST /api/batch/settle
 * Content-Type: application/json
 * { "bizDate": "yesterday", "_requestId": "...", "_dispatchNode": "..." }
 * </pre>
 *
 * <p>WORKFLOW 编排定义通常放在数据库 {@code workflow_def} 字段（见 schema 种子数据
 * {@code nightlyBatchPipeline}），也可通过管理 API 动态创建。
 *
 * @author orbit
 */
@Component
public class RemoteBatchTasks {

    /**
     * 跨服务订单结算：每天凌晨 2 点调用 order-service 的结算接口。
     * 目标服务需在 orbit.scheduler.remote-services.order-service 中注册。
     */
    @BatchTask(name = "remoteOrderSettle",
            description = "跨服务订单结算（REMOTE 派发到 order-service）",
            cron = "0 0 2 * * ?",
            dispatchType = DispatchType.REMOTE,
            httpService = "order-service",
            httpPath = "/api/batch/settle",
            httpMethod = "POST")
    public void remoteOrderSettle() {
        // REMOTE 模式：方法体不会被调用，业务在 order-service 执行
    }

    /**
     * 跨服务库存同步：每小时调用 inventory-service。
     */
    @BatchTask(name = "remoteInventorySync",
            description = "跨服务库存同步（REMOTE 派发到 inventory-service）",
            cron = "0 0 * * * ?",
            dispatchType = DispatchType.REMOTE,
            httpService = "inventory-service",
            httpPath = "/api/batch/sync",
            httpMethod = "POST")
    public void remoteInventorySync() {
        // REMOTE 模式：方法体不会被调用
    }
}
