package com.natpcp.natpcp.dsv2.service.impl;


import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.natpcp.natpcp.dsv2.dao.dto.ConfigurationVersionDTO;
import com.natpcp.natpcp.dsv2.dao.mapper.ConfigurationVersionServiceMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import com.natpcp.natpcp.dsv2.service.ConfigurationVersionService;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class ConfigurationVersionServiceImpl extends
        ServiceImpl<ConfigurationVersionServiceMapper, ConfigurationVersionPo> implements ConfigurationVersionService  {


    @Autowired
    private ConfigurationVersionServiceMapper configurationVersionServiceMapper;

    @Override
    public Integer insert(ConfigurationVersionPo po) {
        return configurationVersionServiceMapper.insert(po);
    }

    @Override
    public Integer delete(ConfigurationVersionPo po) {
        return configurationVersionServiceMapper.delete(po.getConfigName());
    }

    @Override
    public Integer update(ConfigurationVersionPo po) {
        return configurationVersionServiceMapper.update(po);
    }


    @Override
    public ConfigurationVersionPo select(ConfigurationVersionPo po) {
        return configurationVersionServiceMapper.select(po.getConfigName());
    }

    @Override
    public List<ConfigurationVersionDTO> selectAll() {
        return configurationVersionServiceMapper.selectAll();
    }

    @Override
    public List<HashMap<String, String>> selectByConfigName() {
        return configurationVersionServiceMapper.selectByConfigName();
    }
}
