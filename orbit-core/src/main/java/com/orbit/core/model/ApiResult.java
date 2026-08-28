package com.orbit.core.model;

import java.io.Serializable;

/**
 * 统一 HTTP RESTful API 响应包装对象。
 * <p>用于调度中心与执行器、以及调度中心与前端/运维 API 之间的标准交互格式。
 *
 * @param <T> 业务响应数据载荷类型
 */
public class ApiResult<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * HTTP 响应状态码，200 表示成功，400/500 等表示业务或系统错误
     */
    private int code;

    /**
     * 业务操作是否成功的布尔标识（true: 成功，false: 失败）
     */
    private boolean success;

    /**
     * 提示信息或异常错误消息
     */
    private String msg;

    /**
     * 实际返回的业务数据实体
     */
    private T data;

    /**
     * 无参构造方法（反序列化所需）
     */
    public ApiResult() {
    }

    /**
     * 全参构造方法
     *
     * @param code    状态码
     * @param success 是否成功
     * @param msg     消息
     * @param data    数据载荷
     */
    public ApiResult(int code, boolean success, String msg, T data) {
        this.code = code;
        this.success = success;
        this.msg = msg;
        this.data = data;
    }

    /**
     * 快捷构建成功响应（携带数据）
     *
     * @param data 响应数据
     * @param <T>  数据类型
     * @return 成功响应结果
     */
    public static <T> ApiResult<T> ok(T data) {
        return new ApiResult<T>(200, true, "OK", data);
    }

    /**
     * 快捷构建成功响应（无携带数据）
     *
     * @param <T> 数据类型
     * @return 成功响应结果
     */
    public static <T> ApiResult<T> ok() {
        return ok(null);
    }

    /**
     * 快捷构建失败响应（默认 500 状态码）
     *
     * @param msg 错误描述信息
     * @param <T> 数据类型
     * @return 失败响应结果
     */
    public static <T> ApiResult<T> fail(String msg) {
        return new ApiResult<T>(500, false, msg, null);
    }

    /**
     * 快捷构建指定错误码的失败响应
     *
     * @param code 自定义 HTTP 状态码或业务错误码
     * @param msg  错误描述信息
     * @param <T>  数据类型
     * @return 失败响应结果
     */
    public static <T> ApiResult<T> fail(int code, String msg) {
        return new ApiResult<T>(code, false, msg, null);
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
