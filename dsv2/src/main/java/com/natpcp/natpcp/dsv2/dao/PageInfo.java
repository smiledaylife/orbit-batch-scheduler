package com.natpcp.natpcp.dsv2.dao;

import java.util.List;

public class PageInfo<T> {
    // 查询结果列表
    private List<T> results;
    // 当前页数
    private Integer page;
    // 每页记录数
    private Integer pageSize;
    // 总记录数
    private Integer totalCount;

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public List<T> getResults() {
        return results;
    }

    public void setResults(List<T> results) {
        this.results = results;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }
}
