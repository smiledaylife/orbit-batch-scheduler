package com.orbit.scheduler.model;

/**
 * REST 统一响应体。
 *
 * @author orbit
 */
public class ApiResult<T> {

    private int code;
    private boolean success;
    private String message;
    private T data;

    public ApiResult() {
    }

    private ApiResult(int code, boolean success, String message, T data) {
        this.code = code;
        this.success = success;
        this.message = message;
        this.data = data;
    }

    public static <T> ApiResult<T> ok(T data) {
        return new ApiResult<T>(200, true, "OK", data);
    }

    public static <T> ApiResult<T> ok() {
        return new ApiResult<T>(200, true, "OK", null);
    }

    public static <T> ApiResult<T> error(int code, String message) {
        return new ApiResult<T>(code, false, message, null);
    }

    public static <T> ApiResult<T> badRequest(String message) {
        return new ApiResult<T>(400, false, message, null);
    }

    public static <T> ApiResult<T> notFound(String message) {
        return new ApiResult<T>(404, false, message, null);
    }

    public static <T> ApiResult<T> serverError(String message) {
        return new ApiResult<T>(500, false, message, null);
    }

    public int getCode() { return code; }

    public void setCode(int code) { this.code = code; }

    public boolean isSuccess() { return success; }

    public void setSuccess(boolean success) { this.success = success; }

    public String getMessage() { return message; }

    public void setMessage(String message) { this.message = message; }

    public T getData() { return data; }

    public void setData(T data) { this.data = data; }
}
