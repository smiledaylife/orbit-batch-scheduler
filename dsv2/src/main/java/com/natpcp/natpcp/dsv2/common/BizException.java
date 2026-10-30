/*
 * AICode Begin
 * plt-common 等价类：BizException（业务异常）。
 *
 * 等价于 plt-common-bo 的 com.natpcp.common.dsv2.natpcp.BizException。
 * 提供两个构造器：(ResponseStatus) 与 (ResponseStatus, String 附加信息)，
 * 与 plt-mgmt 中大量 `throw new BizException(ExceptionCodeMsg.XXX, msg)` 用法保持一致。
 */
package com.natpcp.natpcp.dsv2.common;

import lombok.Getter;

/**
 * 平台业务异常。
 */
@Getter
public class BizException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 响应状态（错误码/错误信息载体）。
     */
    private final ResponseStatus responseStatus;

    /**
     * 附加错误信息（调用方补充的上下文，如明细错误原因）。
     */
    private final String errorInfo;

    public BizException(ResponseStatus responseStatus) {
        super(null == responseStatus ? "BizException" : responseStatus.getMessage());
        this.responseStatus = responseStatus;
        this.errorInfo = null;
    }

    public BizException(ResponseStatus responseStatus, String errorInfo) {
        super(null == responseStatus ? "BizException" : responseStatus.getMessage(), null == errorInfo ? null : new RuntimeException(errorInfo));
        this.responseStatus = responseStatus;
        this.errorInfo = errorInfo;
    }

    public BizException(ResponseStatus responseStatus, Throwable e) {
        super(null == responseStatus ? "BizException" : responseStatus.getMessage(), e);
        this.responseStatus = responseStatus;
        this.errorInfo = null;
    }
}
/*
 * AICode End
 */
