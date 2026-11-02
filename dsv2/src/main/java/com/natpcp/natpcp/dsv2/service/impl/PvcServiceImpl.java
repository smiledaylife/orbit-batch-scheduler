/*
 * AICode Begin
 * PvcServiceImpl：PVC 文件备份与过期清理能力。
 *
 * 只提供 backupPvcFile() 与 deleteFilePvcTow() 及其私有辅助方法
 * （groupByFile / compressFolder），不依赖任何外部二方 jar。
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
import java.nio.file.StandardCopyOption;
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
                // 关键防线：如果 zip 已存在（上一轮压缩成功但删除原文件时中断后重跑），
                // 此时源目录只剩残余文件，直接重压会用残缺内容覆盖完整备份 → 数据永久丢失。
                // 正确语义：zip 已存在说明备份已成功，本轮只应继续完成删除，禁止重压。
                if (zipFile.exists() && zipFile.length() > 0) {
                    log.warn("备份文件 {} 已存在（上一轮可能已完成压缩但删除中断），跳过重新压缩，仅继续清理源文件",
                            zipFile.getName());
                } else {
                    log.info("开始压缩");
                    // 先写临时文件再原子替换，避免压缩中途失败留下半截 zip 被下轮误判为「完整备份」
                    File tmpZip = new File(backupDir, zipFileName + ".tmp");
                    try (FileOutputStream fos = new FileOutputStream(tmpZip);
                         ZipOutputStream zos = new ZipOutputStream(fos)) {
                        for (PvcfileDTO dto : value) {
                            File fileSource = new File(dto.getFilePath(), dto.getFileName());
                            compressFolder(fileSource, zos, fileSource.getName());
                            log.info(fileSource.getName() + "压缩完成！");
                        }
                        log.info("完成压缩{}", key);
                    } catch (IOException e) {
                        log.error("{} 文件夹压缩失败", key, e);
                        // 清理半截临时文件，避免下轮误判
                        if (!tmpZip.delete()) {
                            log.warn("临时文件清理失败: {}", tmpZip);
                        }
                        throw new IllegalStateException(key + "文件夹压缩失败", e);
                    }
                    // 用 Files.move(REPLACE_EXISTING) 而不是 File.renameTo：
                    // Windows 上 renameTo 在目标文件已存在时直接失败，而本分支恰好可能遇到
                    // 上一轮崩溃留下的 0 字节 zip（只有 zipFile.length() == 0 才会走到这里），
                    // 那样每次重跑都会抛「重命名失败」，备份永远做不完。
                    try {
                        Files.move(tmpZip.toPath(), zipFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        log.error("{} 临时备份文件落盘失败", key, e);
                        if (!tmpZip.delete()) {
                            log.warn("临时文件清理失败: {}", tmpZip);
                        }
                        throw new IllegalStateException("临时备份文件重命名失败: " + tmpZip + " -> " + zipFile, e);
                    }
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
                    // 仅删除文件/空目录（不递归）；删除失败不静默忽略
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
