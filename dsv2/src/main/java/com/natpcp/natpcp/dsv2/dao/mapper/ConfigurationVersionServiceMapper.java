package com.natpcp.natpcp.dsv2.dao.mapper;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.natpcp.natpcp.dsv2.dao.dto.ConfigurationVersionDTO;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import java.util.HashMap;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface ConfigurationVersionServiceMapper extends BaseMapper<ConfigurationVersionPo> {

     /**
      * 新增
      * @param po
      * @return
      */
     int insert(ConfigurationVersionPo po);

     /**
      * 删除
      * @param configName
      * @return
      */
     Integer delete(String configName);

     /**
      * 更新
      * @param po
      * @return
      */
     Integer update(ConfigurationVersionPo po);

     /**
      *查询
      * @param configName
      * @return
      */
     ConfigurationVersionPo select(String configName);

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
