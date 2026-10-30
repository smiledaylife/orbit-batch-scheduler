package com.natpcp.natpcp.dsv2.constant;

import lombok.Getter;

@Getter
public enum CenterEnum {
    /**
     * 中心
     */
    CENTER_HF("hf","合肥中心"),
    CENTER_LF("lf","廊坊中心"),
    CENTER_YZ("yz","亦庄中心");
    private final String value;
    private final String desc;

    CenterEnum(String value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    public static String getDescByValue(String value) {
        CenterEnum[] values = CenterEnum.values();
        for (CenterEnum e : values) {
            if (e.getValue().equals(value)) {
                return e.getDesc();
            }
        }
        return null;
    }
}
