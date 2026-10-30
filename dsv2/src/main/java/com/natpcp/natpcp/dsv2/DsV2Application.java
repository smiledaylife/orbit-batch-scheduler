/*
 * AICode Begin
 * ds-V2 执行器启动类。
 * 职责：
 *  1. 基于 orbit-executor SDK 启动独立执行器（注册到调度中心 orbit-admin）；
 *  2. 扫描 com.natpcp.dsv2 包下的 @OrbitJob 处理器（7 个迁移任务）；
 *  3. 初始化 MyBatis-Plus（Mapper 扫描）与 Spring Boot Web 容器。
 * 说明：业务代码统一归并在 com.natpcp.dsv2 分包（annotation/common/constant/dao/
 * jobs/redis/service/util），启动类置于 com.，默认组件扫描覆盖全部子包。
 */
package com.natpcp.natpcp.dsv2;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * DS-V2 执行器启动入口。
 *
 * <p>运行参数示例：{@code --spring.profiles.active=dev}。</p>
 * <p>说明：
 * 1. 全部业务代码归并到 {@code com.natpcp.dsv2.*} 分包（任务 handler、公共基础层、Redis 层），
 * 2. Mapper 接口统一位于 {@code com.natpcp.dsv2.dao.mapper}，
 * 启动类在 {@code com.}，默认组件扫描 {@code com.**} 覆盖全部 Bean。</p>
 *
 * @author ds-v2
 */
@SpringBootApplication
@MapperScan("com.natpcp.dsv2.dao.mapper")
public class DsV2Application {

    public static void main(String[] args) {
        SpringApplication.run(DsV2Application.class, args);
    }
}
/*
 * AICode End
 */
