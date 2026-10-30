package com.natpcp.natpcp.dsv2.dao.dto;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class PvcfileDTO {
    private String fileName;
    private String fileType;// 文件类型:0-文件夹，1-文件
    private Long fileSize;
    private String updateTime;
    private String filePath;//文件路径
    private String DateYM;

}
