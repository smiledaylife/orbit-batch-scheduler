/*
 * AICode Begin
 * plt-common 等价类：CachePrefix（缓存前缀接口）。
 *
 * 等价于 plt-common-bo 的 com.natpcp.common.dsv2.natpcp.CachePrefix。
 * MgCachePrefixEnum / PltCachePrefix 等枚举实现本接口，用于标识业务缓存 key 前缀。
 */
package com.natpcp.natpcp.dsv2.common;

/**
 * 缓存前缀接口。
 */
public interface CachePrefix {

    /**
     * 获取缓存 key 前缀。
     *
     * @return 前缀字符串
     */
    String getPrefix();
}
/*
 * AICode End
 */
