/*
 * AICode Begin
 * PvcServiceImpl 精简版（仅保留 pvcFileBackup 任务能力）。
 *
 * 原实现 554 行，依赖 PvcMapper / FileRecordPO / SM4Utils / DateUtils / Convert /
 * jsch（ChannelSftp）等；迁移版仅保留 backupPvcFile() 与 deleteFilePvcTow() 及
 * 其私有辅助方法（groupByFile / compressFolder），
 * 去除全部数据库与页面相关依赖，满足「零外部二方 jar」约束。
 */
package com.natpcp.natpcp.dsv2.service.impl;

import com.natpcp.natpcp.dsv2.dao.dto.PvcfileDTO;
import com.natpcp.natpcp.dsv2.service.PvcService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * pvc 文件挂载业务实现（ds-V2 精简版）。
 */
@Service
@Slf4j
public class PvcServiceImpl implements PvcService {

    @Value("${pvc.rootPath:/app/appnatp/transfile/}")
    private String pvcRootPath;

    @Value("${pvc.backup.path}")
    private String backupPath;

    @Value("${plt.localFileDirRoot}")
    private String localFileDirRoot;

    /**
     * 备份 PVC 文件：
     * 将文件按月份文件夹分组，对 3 个月前（月初）的月份文件夹整体压缩到备份目录，然后删除原文件夹。
     */
    @Override
    public void backupPvcFile() {
        log.info("备份开始");
        // 获取 pvc 根路径下按月份分组（目录名 yyyyMMdd 取前 6 位 yyyyMM）
        Map<String, List<PvcfileDTO>> fileMap = groupByFile();
        // 3 个月前月初
        LocalDate localDate = LocalDate.now().minusMonths(3).withDayOfMonth(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        fileMap.forEach((key, value) -> {
            LocalDate date;
            try {
                date = LocalDate.parse(key + "01", formatter);
            } catch (Exception e) {
                log.warn("文件夹名不是日期格式 {}", key);
                throw new IllegalArgumentException("文件夹名不是日期格式{" + key + "}", e);
            }
            if (date.isBefore(localDate)) {
                String zipFileName = key + ".zip";
                File zipFile = new File(backupPath, zipFileName);
                File backupDir = zipFile.getParentFile();
                if (backupDir != null && !backupDir.isDirectory() && !backupDir.mkdirs()) {
                    throw new IllegalStateException("备份目录不存在且无法创建: " + backupDir);
                }
                log.info("开始压缩");
                try (FileOutputStream fos = new FileOutputStream(zipFile);
                     ZipOutputStream zos = new ZipOutputStream(fos)) {
                    for (PvcfileDTO dto : value) {
                        File fileSource = new File(dto.getFilePath(), dto.getFileName());
                        compressFolder(fileSource, zos, fileSource.getName());
                        log.info(fileSource.getName() + "压缩完成！");
                    }
                    log.info("完成压缩{}", key);
                } catch (IOException e) {
                    log.error("{} 文件夹压缩失败", key, e);
                    throw new IllegalStateException(key + "文件夹压缩失败", e);
                }
                // 删除原文件夹（Files.walk 返回的 Stream 必须关闭，否则泄露目录句柄）
                for (PvcfileDTO dto : value) {
                    Path path = new File(dto.getFilePath(), dto.getFileName()).toPath();
                    try (Stream<Path> walk = Files.walk(path)) {
                        List<Path> toDelete = walk.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
                        for (Path p : toDelete) {
                            Files.delete(p);
                        }
                    } catch (IOException e) {
                        log.error("{} 文件删除失败", path, e);
                        throw new IllegalStateException(path + "文件删除失败", e);
                    }
                }
            }
        });
        log.info("备份结束");
    }

    /**
     * 清理 pvc2 路径下 last-modified 超过 3 个月的文件/目录。
     */
    @Override
    public void deleteFilePvcTow() {
        Path directory = Paths.get(localFileDirRoot);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path file : stream) {
                FileTime fileTime = Files.getLastModifiedTime(file);
                LocalDateTime localDateTime = fileTime.toInstant().atZone(ZoneId.systemDefault())
                        .toLocalDateTime().plusMonths(3);
                if (localDateTime.isBefore(LocalDateTime.now())) {
                    // 与原实现语义一致：仅删除文件/空目录（不递归）；但不再静默忽略失败结果
                    try {
                        Files.delete(file);
                        log.info("已清理过期临时文件: {}", file);
                    } catch (java.nio.file.DirectoryNotEmptyException e) {
                        log.warn("过期目录非空，跳过删除: {}", file);
                    } catch (IOException e) {
                        log.warn("过期临时文件删除失败: {}", file, e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("临时文件删除失败", e);
            throw new IllegalStateException("临时文件删除失败", e);
        }
    }

    /**
     * 查询根路径下所有 yyyyMMdd 目录并按 yyyyMM 月份分组。
     *
     * @return key=yyyyMM，value=该月目录下的 PvcfileDTO 列表
     */
    private Map<String, List<PvcfileDTO>> groupByFile() {
        Path directory = Paths.get(pvcRootPath);
        List<PvcfileDTO> records = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path file : stream) {
                if (Files.isDirectory(file) && file.getFileName().toString().matches("\\d{8}")) {
                    PvcfileDTO pvcfileDTO = new PvcfileDTO();
                    pvcfileDTO.setFileName(file.getFileName().toString())
                            .setFilePath(pvcRootPath)
                            .setDateYM(file.getFileName().toString().substring(0, 6));
                    records.add(pvcfileDTO);
                }
            }
        } catch (Exception e) {
            log.error("读取 pvc 根目录 {} 失败", pvcRootPath, e);
            throw new IllegalStateException("读取 pvc 根目录失败: " + pvcRootPath, e);
        }
        return records.stream().collect(Collectors.groupingBy(PvcfileDTO::getDateYM));
    }

    /**
     * 递归压缩文件夹/文件到 zip 输出流。
     */
    private void compressFolder(File folder, ZipOutputStream zos, String parentPath) throws IOException {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    String newPath = parentPath + File.separator + file.getName();
                    compressFolder(file, zos, newPath);
                } else {
                    String entryPath = parentPath + File.separator + file.getName();
                    ZipEntry entry = new ZipEntry(entryPath);
                    zos.putNextEntry(entry);
                    try (FileInputStream fis = new FileInputStream(file)) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = fis.read(buffer)) > 0) {
                            zos.write(buffer, 0, len);
                        }
                    }
                    zos.closeEntry();
                }
            }
        }
    }
}
/*
 * AICode End
 */
