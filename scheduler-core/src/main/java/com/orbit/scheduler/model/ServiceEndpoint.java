package com.orbit.scheduler.model;

/**
 * 服务访问端点（例如普通 K8s Service 的 ClusterIP 或 Headless Service 的 Pod 地址）。
 *
 * @author orbit
 */
public class ServiceEndpoint {

    private final String url;

    public ServiceEndpoint(String url) {
        this.url = url;
    }

    /** 形如 http://10.244.1.17:8080，也可以是普通 Service 的 ClusterIP。 */
    public String getUrl() { return url; }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        return url.equals(((ServiceEndpoint) o).url);
    }

    @Override
    public int hashCode() { return url.hashCode(); }

    @Override
    public String toString() { return url; }
}
