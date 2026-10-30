package com.natpcp.natpcp.dsv2.service.impl;

import com.natpcp.natpcp.dsv2.dao.dto.ConfigurationVersionDTO;
import com.natpcp.natpcp.dsv2.dao.mapper.ConfigurationVersionServiceMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import com.natpcp.natpcp.dsv2.service.impl.ConfigurationVersionServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * ConfigurationVersionServiceImpl unit test.
 *
 * 注意：被测类继承 ServiceImpl 并覆写 insert 等方法，本测试只关注覆写方法的行为。
 */
@DisplayName("ConfigurationVersionServiceImpl Test")
@ExtendWith(MockitoExtension.class)
class ConfigurationVersionServiceImplTest {

    @Mock
    private ConfigurationVersionServiceMapper configurationVersionServiceMapper;

    @InjectMocks
    private ConfigurationVersionServiceImpl configurationVersionService;

    private ConfigurationVersionPo po;

    @BeforeEach
    void setUp() {
        po = new ConfigurationVersionPo();
        po.setConfigName("cfg1");
        po.setVersion("v1");
    }

    @DisplayName("insert: 委托 mapper.insert 并返回行数")
    @Test
    void insert_shouldDelegate() {
        when(configurationVersionServiceMapper.insert(po)).thenReturn(1);
        assertEquals(Integer.valueOf(1), configurationVersionService.insert(po));
        verify(configurationVersionServiceMapper).insert(po);
    }

    @DisplayName("delete: 按 configName 委托 mapper.delete")
    @Test
    void delete_shouldDelegateByConfigName() {
        when(configurationVersionServiceMapper.delete("cfg1")).thenReturn(1);
        assertEquals(Integer.valueOf(1), configurationVersionService.delete(po));
        verify(configurationVersionServiceMapper).delete("cfg1");
    }

    @DisplayName("update: 委托 mapper.update")
    @Test
    void update_shouldDelegate() {
        when(configurationVersionServiceMapper.update(po)).thenReturn(1);
        assertEquals(Integer.valueOf(1), configurationVersionService.update(po));
        verify(configurationVersionServiceMapper).update(po);
    }

    @DisplayName("select: 按 configName 委托 mapper.select")
    @Test
    void select_shouldDelegateByConfigName() {
        ConfigurationVersionPo byName = new ConfigurationVersionPo();
        byName.setConfigName("cfg1");
        when(configurationVersionServiceMapper.select("cfg1")).thenReturn(byName);
        assertSame(byName, configurationVersionService.select(po));
        verify(configurationVersionServiceMapper).select("cfg1");
    }

    @DisplayName("selectAll: 委托 mapper.selectAll")
    @Test
    void selectAll_shouldDelegate() {
        ConfigurationVersionDTO dto = new ConfigurationVersionDTO();
        List<ConfigurationVersionDTO> list = Arrays.asList(dto);
        when(configurationVersionServiceMapper.selectAll()).thenReturn(list);
        assertSame(list, configurationVersionService.selectAll());
        verify(configurationVersionServiceMapper).selectAll();
    }

    @DisplayName("selectByConfigName: 委托 mapper.selectByConfigName")
    @Test
    void selectByConfigName_shouldDelegate() {
        HashMap<String, String> m = new HashMap<>();
        m.put("cfg1", "v1");
        List<HashMap<String, String>> list = Arrays.asList(m);
        when(configurationVersionServiceMapper.selectByConfigName()).thenReturn(list);
        assertSame(list, configurationVersionService.selectByConfigName());
        verify(configurationVersionServiceMapper).selectByConfigName();
    }
}
/*
 * AICode End
 */
