package com.orbit.admin.config;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.OptimisticLockerInnerInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MyBatis-Plus 配置。
 * 核心职责：
 * 
 *   - 通过 {@link MapperScan} 扫描持久层 Mapper 接口（{@code com.orbit.admin.store.mapper}）；
 *   - 注册 {@link MybatisPlusInterceptor}，含分页插件（{@link PaginationInnerInterceptor}）
 *       与乐观锁插件（{@link OptimisticLockerInnerInterceptor}，配合实体上的 {@code @Version} 字段）。
 * 
 * 数据库方言：H2 / PostgreSQL / openGauss(GaussDB) 均走 PostgreSQL 方言，统一指定 {@link DbType#POSTGRE_SQL}。
 */
@Configuration
@MapperScan("com.orbit.admin.store.mapper")
public class MybatisPlusConfig {

    /**
     * 注册 MyBatis-Plus 核心拦截器链。
     * 注意：多 InnerInterceptor 时，分页插件需放在乐观锁插件之前（官方推荐顺序）。
     *
     * @return MyBatis-Plus 拦截器
     */
    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        // 1. 分页插件：PostgreSQL 方言（H2 使用 PostgreSQL 兼容模式，GaussDB/openGauss 亦兼容该方言）
        PaginationInnerInterceptor pagination = new PaginationInnerInterceptor(DbType.POSTGRE_SQL);
        // 单页上限保护，避免误传超大 size
        pagination.setMaxLimit(200L);
        interceptor.addInnerInterceptor(pagination);
        // 2. 乐观锁插件：更新带 @Version 字段时自动拼接 version 条件并自增
        interceptor.addInnerInterceptor(new OptimisticLockerInnerInterceptor());
        return interceptor;
    }
}
