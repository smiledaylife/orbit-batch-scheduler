package com.orbit.core.model;

/**
 * 统一 HTTP 响应。
 */
public class ApiResult<T> {

    private int code;
    private boolean success;
    private String msg;
    private T data;

    public ApiResult() {
    }

    public ApiResult(int code, boolean success, String msg, T data) {
        this.code = code;
        this.success = success;
        this.msg = msg;
        this.data = data;
    }

    public static <T> ApiResult<T> ok(T data) {
        return new ApiResult<T>(200, true, "OK", data);
    }

    public static <T> ApiResult<T> ok() {
        return ok(null);
    }

    public static <T> ApiResult<T> fail(String msg) {
        return new ApiResult<T>(500, false, msg, null);
    }

    public static <T> ApiResult<T> fail(int code, String msg) {
        return new ApiResult<T>(code, false, msg, null);
    }

    public int getCode() { return code; }
    public void setCode(int code) { this.code = code; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getMsg() { return msg; }
    public void setMsg(String msg) { this.msg = msg; }
    public T getData() { return data; }
    public void setData(T data) { this.data = data; }
}
