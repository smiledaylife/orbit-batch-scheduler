package com.orbit.core.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 分页数据响应包装对象。
 * <p>用于调度中心任务列表、调度日志等分页查询接口的数据返回。
 *
 * @param <T> 分页列表元素类型
 */
public class PageResult<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 当前页码（从 1 开始）
     */
    private int page;

    /**
     * 每页期望展示的记录数（PageSize）
     */
    private int size;

    /**
     * 符合查询条件的总记录数
     */
    private long total;

    /**
     * 当前页包含的数据记录列表
     */
    private List<T> items = new ArrayList<T>();

    /**
     * 无参构造函数（JSON 反序列化所需）
     */
    public PageResult() {
    }

    /**
     * 全参构造函数
     *
     * @param page  当前页码
     * @param size  每页大小
     * @param total 数据总记录数
     * @param items 当前页数据集
     */
    public PageResult(int page, int size, long total, List<T> items) {
        this.page = page;
        this.size = size;
        this.total = total;
        this.items = items == null ? new ArrayList<T>() : items;
    }

    /**
     * 获取当前页码
     *
     * @return 当前页码
     */
    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    /**
     * 获取每页记录数
     *
     * @return 每页记录数
     */
    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    /**
     * 获取总记录数
     *
     * @return 总记录数
     */
    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    /**
     * 获取当前页列表数据
     *
     * @return 列表数据
     */
    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
    }
}
