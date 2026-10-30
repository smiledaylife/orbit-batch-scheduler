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
import java.util.List;

/**
 * 通知消息服务实现（ds-V2 精简版）。
 */
@Service
@Slf4j
public class NoticeServiceImpl implements NoticeService {

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
    public void clearNotice(LocalDate localDate) {
        noticeMapper.clearNotice(localDate);
    }
}
/*
 * AICode End
 */
