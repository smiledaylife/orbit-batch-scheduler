package com.natpcp.natpcp.dsv2.constant;

import lombok.Getter;

@Getter
public enum LocationEnum {
    /**
     * 区域
     */
    LOCATION_WEB("web","web区"),
    LOCATION_APP("app","app区"),
    LOCATION_CORE("core","核心区");

    private final String value;
    private final String desc;

    LocationEnum(String value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    public static String getDescByValue(String value) {
        LocationEnum[] values = LocationEnum.values();
        for (LocationEnum e : values) {
            if (e.getValue().equals(value)) {
                return e.getDesc();
            }
        }
        return null;
    }
}
