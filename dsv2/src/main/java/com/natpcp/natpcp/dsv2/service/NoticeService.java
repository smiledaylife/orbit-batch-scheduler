/*
 * AICode Begin
 * NoticeService 精简版（仅保留 7 个迁移任务用到的能力）。
 *
 * 原接口继承 MyBatis-Plus IService 且引入 UserContext/SocketUtil/WebSocketMessage 等
 * 二方/页内依赖；迁移版仅保留：单条/批量插入通知、按日期清理通知（clearNotice 任务用）。
 */
package com.natpcp.natpcp.dsv2.service;

import com.natpcp.natpcp.dsv2.dao.po.NoticePO;

import java.time.LocalDate;
import java.util.List;

/**
 * 通知消息服务（ds-V2 精简版，供 versionNotice/clearNotice 任务使用）。
 */
public interface NoticeService {

    /**
     * 新增通知。
     *
     * @param noticePO 通知实体
     */
    void insert(NoticePO noticePO);

    /**
     * 批量新增通知。
     *
     * @param noticePOList 通知实体列表
     */
    void insert(List<NoticePO> noticePOList);

    /**
     * 向系统管理员（角色 0101）与运维员（角色 0104）各发送一条巡检/告警通知。
     *
     * <p>所有定时任务的告警通知均走此方法，统一 id 生成、状态、角色等字段填充。</p>
     *
     * @param title   通知标题
     * @param content 通知内容
     */
    void sendToAdminAndOps(String title, String content);

    /**
     * 定时删除通知记录。
     *
     * @param localDate 删除 create_time 早于该日期的历史通知
     */
    void clearNotice(LocalDate localDate);
}
/*
 * AICode End
 */
