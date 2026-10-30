/*
 * AICode Begin
 * plt-common 等价类：ResponseStatus（响应状态接口）。
 *
 * 原 plt-common-bo 二方库不可用（内网 Maven 仓库无该 jar），
 * 为满足「零外部二方 jar」约束，以源码形式在本工程内重建等价接口，
 * 保持原包名 com.natpcp.dsv2.common.bo，供 ExceptionCodeMsg / PltResponseStatus / BizException 使用。
 */
package com.natpcp.natpcp.dsv2.common;

/**
 * 平台标准响应状态接口（等价于 plt-common-bo 的 com.natpcp.common.dsv2.natpcp.ResponseStatus）。
 */
public interface ResponseStatus {

    /**
     * 返回给前端的响应码。
     *
     * @return 响应码
     */
    String getCode();

    /**
     * 返回给前端的子响应码。
     *
     * @return 子响应码
     */
    String getSubCode();

    /**
     * 返回给前端的响应信息。
     *
     * @return 响应信息
     */
    String getMessage();

    /**
     * 处理建议编号。
     *
     * @return 处理建议编号
     */
    String getResolutionNo();
}
/*
 * AICode End
 */
