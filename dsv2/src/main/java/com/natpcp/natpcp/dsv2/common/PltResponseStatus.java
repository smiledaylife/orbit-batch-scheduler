/*
 * AICode Begin
 * plt-common 等价类：PltResponseStatus（平台通用响应状态枚举）。
 *
 * 等价于 plt-common-bo 的 com.natpcp.common.dsv2.natpcp.PltResponseStatus。
 * 仅保留本次 7 个迁移任务实际用到的枚举项（versionNotice 任务的参数错误），
 * 其余按需补充。
 */
package com.natpcp.natpcp.dsv2.common;

/**
 * 平台通用响应状态枚举。
 */
public enum PltResponseStatus implements ResponseStatus {

    /**
     * 参数错误
     */
    PARAMETER_ERROR("YPA", "0B0001", "参数错误!", ""),

    /**
     * 系统异常
     */
    SYS_ERROR("11111111", "", "系统异常", "");

    private String code;
    private String subCode;
    private String message;
    private String resolutionNo;

    PltResponseStatus(String code, String subCode, String message, String resolutionNo) {
        this.code = code;
        this.subCode = subCode;
        this.message = message;
        this.resolutionNo = resolutionNo;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getSubCode() {
        return subCode;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public String getResolutionNo() {
        return resolutionNo;
    }
}
/*
 * AICode End
 */
