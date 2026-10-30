package com.natpcp.natpcp.dsv2.constant;


import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;

import java.lang.reflect.Field;

public class ClassInfo {
    public static String getTableName(Class<?> clazz) {
        TableName table = clazz.getAnnotation(TableName.class);
        return (table != null) ? table.value() : clazz.getSimpleName();
    }

    public static String getColumnName(Field field) {
        TableField column = field.getAnnotation(TableField.class);
        if (column != null && !column.value().isEmpty()) {
            return column.value();
        }
        // 与 MyBatis-Plus 默认 map-underscore-to-camel-case 规则一致：驼峰字段名 -> 下划线列名
        return camelToUnderscore(field.getName());
    }

    private static String camelToUnderscore(String name) {
        StringBuilder sb = new StringBuilder(name.length() + 4);
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                sb.append('_');
            }
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

}
