package com.natpcp.natpcp.dsv2.service.impl;

import com.natpcp.natpcp.dsv2.constant.ClassInfo;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import org.apache.ibatis.jdbc.SQL;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.stream.Collectors;


public class DynamicSqlProvider {

    public String query(Class<?> clazz, Map<String, Object> params, Map<String, Object> conditions, Integer page, Integer pageSize) {
        if (!clazz.isAssignableFrom(ConfigurationVersionPo.class) && conditions != null) {
            conditions.put("delStatus", "1");
        }
        String sql = new SQL() {{
            SELECT(params != null && !params.isEmpty() ?
                    params.keySet().stream()
                            .map(key -> {
                                try {
                                    Field field = clazz.getDeclaredField(key);
                                    field.setAccessible(true);
                                    return ClassInfo.getColumnName(field);
                                } catch (NoSuchFieldException e) {
                                    e.printStackTrace();
                                    return null;
                                }
                            })
                            .collect(Collectors.joining(","))
                    : "*");
            FROM(ClassInfo.getTableName(clazz));
            String emptyString = "";
            if (conditions != null && !conditions.isEmpty()) {
                conditions.forEach((key, value) -> {
                    if (value != null && !value.equals(emptyString)) {  // 检查值非空
                        try {
                            Field field = clazz.getDeclaredField(key);
                            field.setAccessible(true);
                            WHERE(ClassInfo.getColumnName(field) + " = '" + value + "'");
                        } catch (NoSuchFieldException e) {
                            e.printStackTrace();
                        }
                    }
                });
            } else {
                if (!clazz.isAssignableFrom(ConfigurationVersionPo.class)) {
                    WHERE("del_status = '1'");
                }
            }
            if(null != page && null != pageSize && "tb_macrelated_txcode".equals(ClassInfo.getTableName(clazz))){
                ORDER_BY("last_mod_time desc");
            }
            if (null != page && null != pageSize) {
                OFFSET(page * pageSize);
                LIMIT(pageSize);
            }
        }}.toString();
        return sql;
    }

}

