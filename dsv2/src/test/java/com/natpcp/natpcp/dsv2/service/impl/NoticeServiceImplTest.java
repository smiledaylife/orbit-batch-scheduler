package com.natpcp.natpcp.dsv2.service.impl;

import com.natpcp.natpcp.dsv2.dao.mapper.NoticeMapper;
import com.natpcp.natpcp.dsv2.dao.po.NoticePO;
import com.natpcp.natpcp.dsv2.service.impl.NoticeServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * AICode Begin
 * NoticeServiceImpl unit test.
 */
@DisplayName("NoticeServiceImpl Test")
@ExtendWith(MockitoExtension.class)
class NoticeServiceImplTest {

    @Mock
    private NoticeMapper noticeMapper;

    @InjectMocks
    private NoticeServiceImpl noticeService;

    private NoticePO po;

    @BeforeEach
    void setUp() {
        po = new NoticePO();
        po.setId("1");
    }

    @DisplayName("insert(single): 委托 mapper.insert")
    @Test
    void insert_shouldDelegate_whenSingle() {
        noticeService.insert(po);
        verify(noticeMapper).insert(po);
    }

    @DisplayName("insert(list): null 直接返回")
    @Test
    void insert_shouldReturn_whenListNull() {
        noticeService.insert((List<NoticePO>) null);
        verifyNoInteractions(noticeMapper);
    }

    @DisplayName("insert(list): 空列表直接返回")
    @Test
    void insert_shouldReturn_whenListEmpty() {
        noticeService.insert(Collections.emptyList());
        verifyNoInteractions(noticeMapper);
    }

    @DisplayName("insert(list): 逐个委托 mapper.insert")
    @Test
    void insert_shouldDelegateEach_whenListNotEmpty() {
        NoticePO po2 = new NoticePO();
        po2.setId("2");
        List<NoticePO> list = Arrays.asList(po, po2);
        noticeService.insert(list);
        verify(noticeMapper).insert(po);
        verify(noticeMapper).insert(po2);
    }

    @DisplayName("clearNotice: 委托 mapper.clearNotice")
    @Test
    void clearNotice_shouldDelegate() {
        LocalDate date = LocalDate.of(2026, 1, 1);
        noticeService.clearNotice(date);
        verify(noticeMapper).clearNotice(date);
    }
}
/*
 * AICode End
 */
