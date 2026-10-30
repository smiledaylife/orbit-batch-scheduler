package com.natpcp.natpcp.dsv2.dao.mapper;

import com.natpcp.natpcp.dsv2.service.impl.DynamicSqlProvider;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

@Mapper
public interface GenericMapper<T> {


    /**
     * 查询
     *
     * @param clazz
     * @param params
     * @param conditions
     * @param page
     * @param pageSize
     * @return
     */
    @SelectProvider(type = DynamicSqlProvider.class, method = "query")
    List<Map<String, Object>> query(Class<T> clazz, Map<String, Object> params,
                                    Map<String, Object> conditions, Integer page, Integer pageSize);


}
