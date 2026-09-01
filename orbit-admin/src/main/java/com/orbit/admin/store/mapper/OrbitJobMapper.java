package com.orbit.admin.store.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.orbit.admin.store.po.OrbitJobPO;

/**
 * 任务定义表 {@code orbit_job} 的 MyBatis-Plus Mapper。
 * 继承 {@link BaseMapper} 即拥有单表 CRUD；复杂查询通过 QueryWrapper/LambdaQueryWrapper 构建。
 */
public interface OrbitJobMapper extends BaseMapper<OrbitJobPO> {
}
