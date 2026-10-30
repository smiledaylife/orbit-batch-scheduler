/*
 * AICode Begin
 * PvcService 精简版（仅保留 pvcFileBackup 任务能力）。
 *
 * 原接口包含 getFilesByPath/downLoad/upLoad/createDirectory/delete/queryFileRecord
 * 等页面交互能力（依赖 PvcMapper/FileRecordPO/SM4Utils 等二方依赖）；
 * 迁移版仅保留任务使用的 backupPvcFile() / deleteFilePvcTow()。
 */
package com.natpcp.natpcp.dsv2.service;

/**
 * pvc 文件挂载业务层（ds-V2 精简版）。
 */
public interface PvcService {

    /**
     * 定时清理并备份 3 个月前 pvc 路径下文件（按月分组压缩后删除原文件）。
     */
    void backupPvcFile();

    /**
     * 定时清理 3 个月前 pvc2 路径下文件。
     */
    void deleteFilePvcTow();
}
/*
 * AICode End
 */
