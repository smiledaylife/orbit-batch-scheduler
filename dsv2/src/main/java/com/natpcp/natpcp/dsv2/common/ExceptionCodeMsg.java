package com.natpcp.natpcp.dsv2.common;

/**
 * @Description:
 * @Author: Xiongke
 * @Date: 2022-7-29 15:51
 */
public enum ExceptionCodeMsg implements ResponseStatus {

    /**
     * 定时消息通知失败
     */
    TRADE_NOTICE_ERROR("MAC", "0000115", "定时消息通知失败", ""),
    ;

    ExceptionCodeMsg(String code, String subCode, String message, String resolutionNo) {
        this.code = code;
        this.subCode = subCode;
        this.message = message;
        this.resolutionNo = resolutionNo;
    }

    private String code;
    private String subCode;
    private String message;
    private String resolutionNo;

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
