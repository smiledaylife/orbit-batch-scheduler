package com.natpcp.natpcp.dsv2.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.natpcp.natpcp.dsv2.dao.po.NoticePO;
import java.time.LocalDate;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface NoticeMapper extends BaseMapper<NoticePO> {

    int insert(NoticePO noticePO);

    void clearNotice(@Param("localDate") LocalDate localDate);

}
