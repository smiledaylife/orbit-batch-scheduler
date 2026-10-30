package com.natpcp.natpcp.dsv2.service;

import com.natpcp.natpcp.dsv2.dao.dto.ConfigurationVersionDTO;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;

import java.util.HashMap;
import java.util.List;

public interface ConfigurationVersionService {

    /**
     * 新增
     * @param po
     * @return
     */
    Integer insert(ConfigurationVersionPo po);

    /**
     * 删除
     * @param po
     * @return
     */
    Integer delete(ConfigurationVersionPo po);

    /**
     * 更新
     * @param po
     * @return
     */
    Integer update(ConfigurationVersionPo po);

    /**
     * 查询
     * @param po
     * @return
     */
    ConfigurationVersionPo select(ConfigurationVersionPo po);

    /**
     * 查询所有
     * @return
     */
    List<ConfigurationVersionDTO> selectAll();

    /**
     * 查询
     * @return
     */
    List<HashMap<String, String>> selectByConfigName();
}
