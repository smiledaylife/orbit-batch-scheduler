/*
 * AICode Begin
 * NoticeServiceImpl 精简版。
 *
 * 原实现继承 MyBatis-Plus ServiceImpl 并依赖 UserContext / SocketUtil /
 * NoticeForSearchDTO / WebSocketMessage；迁移版去除全部页内/上下文依赖，
 * 仅基于 NoticeMapper（XML 提供 insert / clearNotice）实现任务所需能力。
 */
package com.natpcp.natpcp.dsv2.service.impl;

import com.natpcp.natpcp.dsv2.dao.mapper.NoticeMapper;
import com.natpcp.natpcp.dsv2.dao.po.NoticePO;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.List;

/**
 * 通知消息服务实现（ds-V2 精简版）。
 */
@Service
@Slf4j
public class NoticeServiceImpl implements NoticeService {

    /** 告警通知接收角色：0101 系统管理员、0104 运维员 */
    private static final List<String> ALERT_RECEIVE_ROLES = List.of("0101", "0104");

    /** 通知状态：未读 */
    private static final String NOTICE_STATUS_UNREAD = "0";

    @Autowired
    private NoticeMapper noticeMapper;

    @Override
    public void insert(NoticePO noticePO) {
        noticeMapper.insert(noticePO);
    }

    @Override
    public void insert(List<NoticePO> noticePOList) {
        if (CollectionUtils.isEmpty(noticePOList)) {
            return;
        }
        for (NoticePO noticePO : noticePOList) {
            noticeMapper.insert(noticePO);
        }
    }

    @Override
    public void sendToAdminAndOps(String title, String content) {
        for (String role : ALERT_RECEIVE_ROLES) {
            NoticePO po = new NoticePO();
            po.setId(UUID.randomUUID().toString());
            po.setBusinessId("");
            po.setTitle(title);
            po.setContent(content);
            po.setStatus(NOTICE_STATUS_UNREAD);
            po.setReceiveTellerNo("");
            po.setTellerName("");
            po.setReceiveRole(role);
            po.setCreateTime(LocalDateTime.now());
            noticeMapper.insert(po);
        }
    }

    @Override
    public void clearNotice(LocalDate localDate) {
        noticeMapper.clearNotice(localDate);
    }
}
/*
 * AICode End
 */
