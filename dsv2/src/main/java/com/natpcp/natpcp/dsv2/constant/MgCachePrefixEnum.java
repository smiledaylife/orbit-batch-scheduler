package com.natpcp.natpcp.dsv2.constant;

import com.natpcp.natpcp.dsv2.common.CachePrefix;

public enum MgCachePrefixEnum implements CachePrefix {

    //plt:function:urls
    FUNCTION_URLS("plt:function:urls");

    private String prefix;

    private MgCachePrefixEnum(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return this.prefix;
    }
}
