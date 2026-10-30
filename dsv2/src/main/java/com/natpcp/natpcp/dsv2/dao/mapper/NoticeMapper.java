package com.natpcp.natpcp.dsv2.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.natpcp.natpcp.dsv2.dao.po.NoticePO;
import java.time.LocalDate;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface NoticeMapper extends BaseMapper<NoticePO> {

    int insert(NoticePO noticePO);

    List<NoticePO> listNoticePOByNumber(@Param("number") Integer number,
            @Param("receiveRole")String receiveRole);

    List<NoticePO> listNoticePO(@Param("receiveRole")String receiveRole,@Param("status")String status);


    Integer numberUnReadNotice(@Param("receiveRole")String receiveRole);

    Integer updateSetStatus(@Param("id")String id);


    void oneClickReadNotice(@Param("receiveRole") String receiveRole);

    void clearNotice(@Param("localDate") LocalDate localDate);

}
