/*
 * AICode Begin
 * DynamicSqlProvider：GenericMapper.query 的动态 SQL 提供者。
 *
 * 安全约束：
 *   - 列名只能来自实体字段上的 @TableField / 字段名（反射自实体类，不来自外部输入）；
 *   - 条件值一律通过 MyBatis 参数占位符 #{conditions.key} 绑定，禁止字符串拼接，
 *     避免 SQL 注入（历史实现为 "col = 'value'" 拼接）。
 *   - 该 provider 依赖 maven-compiler-plugin 的 -parameters（GenericMapper 未使用 @Param，
 *     MyBatis 按形参名 clazz/params/conditions/page/pageSize 匹配）。
 */
package com.natpcp.natpcp.dsv2.service.impl;

import com.natpcp.natpcp.dsv2.constant.ClassInfo;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.jdbc.SQL;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 通用查询 SQL 构建器。
 */
@Slf4j
public class DynamicSqlProvider {

    /** 允许作为 #{conditions.xxx} 占位符一部分的 key（Java 标识符） */
    private static final Pattern SAFE_KEY = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    public String query(Class<?> clazz, Map<String, Object> params, Map<String, Object> conditions,
                        Integer page, Integer pageSize) {
        boolean hasDelStatus = !clazz.isAssignableFrom(ConfigurationVersionPo.class);
        if (hasDelStatus && conditions != null) {
            conditions.put("delStatus", "1");
        }
        return new SQL() {{
            SELECT(params != null && !params.isEmpty()
                    ? params.keySet().stream()
                            .map(key -> resolveColumn(clazz, key))
                            .filter(col -> col != null)
                            .collect(Collectors.joining(","))
                    : "*");
            FROM(ClassInfo.getTableName(clazz));
            if (conditions != null && !conditions.isEmpty()) {
                conditions.forEach((key, value) -> {
                    if (value == null || "".equals(value)) {
                        return;
                    }
                    if (!SAFE_KEY.matcher(key).matches()) {
                        throw new IllegalArgumentException("非法的查询条件键: " + key);
                    }
                    String column = resolveColumn(clazz, key);
                    if (column != null) {
                        // 值通过 MyBatis 参数绑定，不拼接进 SQL
                        WHERE(column + " = #{conditions." + key + "}");
                    }
                });
            } else if (hasDelStatus) {
                WHERE("del_status = '1'");
            }
            if (page != null && pageSize != null) {
                if ("tb_macrelated_txcode".equals(ClassInfo.getTableName(clazz))) {
                    ORDER_BY("last_mod_time desc");
                }
                OFFSET((long) page * pageSize);
                LIMIT(pageSize);
            }
        }}.toString();
    }

    /**
     * 将实体字段名解析为列名；字段不存在时返回 null（并记录日志）。
     */
    private static String resolveColumn(Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            return ClassInfo.getColumnName(field);
        } catch (NoSuchFieldException e) {
            log.warn("实体 {} 不存在字段 {}，忽略该列/条件", clazz.getSimpleName(), fieldName);
            return null;
        }
    }
}
/*
 * AICode End
 */
