/*
 * AICode Begin
 * UpdateRedisSyncItem：updateRedisInfo 任务 chunk 化的处理单元。
 *
 * 每个 item 对应一个"Redis 目标 key 的全量同步动作"：
 *   - redisKey   ：目标 Redis key（如 macRelated / inRoute / outRoute ...）
 *   - clazz      ：对应的数据库实体类型（用于 GenericMapper 全表查询）
 *   - needUpdate ：Processor 阶段完成 数据库比对 Redis 后标记，Writer 仅处理差异项
 *   - dataString ：数据库侧序列化结果（JSON 字符串），由 Processor 计算
 */
package com.natpcp.natpcp.dsv2.batch.item;

/**
 * updateRedisInfo 任务 chunk 处理单元。
 */
public class UpdateRedisSyncItem {

    private final String redisKey;

    private final Class<?> clazz;

    private boolean needUpdate;

    private String dataString;

    public UpdateRedisSyncItem(String redisKey, Class<?> clazz) {
        this.redisKey = redisKey;
        this.clazz = clazz;
    }

    public String getRedisKey() {
        return redisKey;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public boolean isNeedUpdate() {
        return needUpdate;
    }

    public void setNeedUpdate(boolean needUpdate) {
        this.needUpdate = needUpdate;
    }

    public String getDataString() {
        return dataString;
    }

    public void setDataString(String dataString) {
        this.dataString = dataString;
    }
}
/*
 * AICode End
 */
