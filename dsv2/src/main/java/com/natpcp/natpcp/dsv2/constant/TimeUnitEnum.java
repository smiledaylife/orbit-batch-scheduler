package com.natpcp.natpcp.dsv2.constant;

/**
 * @Auther wangl
 * @Date : 2024/3/15  10:12
 */
public enum TimeUnitEnum {
    /**
     * 年
     */
    YEAR(0),
    /**
     *月
     */
    MONTH(1),
    /**
     *周
     */
    WEEK(2),
    /**
     *天
     */
    DAY(3),
    /**
     *小时
     */
    HOUR(4),
    /**
     *分
     */
    MINUTE(5),
    /**
     *秒
     */
    SECOND(6);

    public Integer unit;

    TimeUnitEnum(Integer unit) {
        this.unit = unit;
    }

    TimeUnitEnum() {
    }

    public static TimeUnitEnum getByUnit(Integer unit){
        for (TimeUnitEnum value : TimeUnitEnum.values()) {
            if (value.unit.equals(unit)) {
                return value;
            }
        }
        return null;
    }
}
