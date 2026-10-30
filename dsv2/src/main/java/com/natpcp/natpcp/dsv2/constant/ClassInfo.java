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
        return (column != null) ? column.value() : field.getName();
    }

}
