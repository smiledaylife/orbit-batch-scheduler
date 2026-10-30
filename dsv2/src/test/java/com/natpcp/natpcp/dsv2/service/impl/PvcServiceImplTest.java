package com.natpcp.natpcp.dsv2.service.impl;

import com.natpcp.natpcp.dsv2.service.impl.PvcServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * AICode Begin
 * PvcServiceImpl unit test（借助 JUnit @TempDir 验证目录分组/压缩/清理逻辑）。
 */
@DisplayName("PvcServiceImpl Test")
class PvcServiceImplTest {

    @TempDir
    Path tmpDir;

    private PvcServiceImpl service;
    private Path pvcRoot;
    private Path backupDir;
    private Path localDir;

    @BeforeEach
    void setUp() throws Exception {
        service = new PvcServiceImpl();
        pvcRoot = tmpDir.resolve("pvcRoot");
        backupDir = tmpDir.resolve("backup");
        localDir = tmpDir.resolve("local");
        Files.createDirectories(pvcRoot);
        Files.createDirectories(backupDir);
        Files.createDirectories(localDir);
        ReflectionTestUtils.setField(service, "pvcRootPath", pvcRoot.toString() + java.io.File.separator);
        ReflectionTestUtils.setField(service, "backupPath", backupDir.toString() + java.io.File.separator);
        ReflectionTestUtils.setField(service, "localFileDirRoot", localDir.toString());
    }

    @DisplayName("backupPvcFile: 3 个月前日期目录被压缩并删除原目录")
    @Test
    void backupPvcFile_shouldZipAndDeleteOldMonth() throws Exception {
        Path oldDir = pvcRoot.resolve("20260110");
        Files.createDirectories(oldDir);
        Files.write(oldDir.resolve("a.txt"), "hello".getBytes());
        // 日期早于 3 个月前
        service.backupPvcFile();
        // 备份目录应出现 202601.zip
        Path zip = backupDir.resolve("202601.zip");
        assertThat(zip).exists();
        // 原月份目录被删除
        assertThat(oldDir).doesNotExist();
    }

    @DisplayName("backupPvcFile: 近期日期目录不备份不删除")
    @Test
    void backupPvcFile_shouldSkipRecentMonth() throws Exception {
        String ym = java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMM"));
        Path recentDir = pvcRoot.resolve(ym + "01");
        Files.createDirectories(recentDir);
        Files.write(recentDir.resolve("b.txt"), "x".getBytes());
        service.backupPvcFile();
        assertThat(recentDir).exists();
        assertThat(backupDir.resolve(ym + ".zip")).doesNotExist();
    }

    @DisplayName("backupPvcFile: 非 8 位数字目录被忽略")
    @Test
    void backupPvcFile_shouldIgnoreNonDateDir() throws Exception {
        Files.createDirectories(pvcRoot.resolve("notadate"));
        service.backupPvcFile();
        assertThat(pvcRoot.resolve("notadate")).exists();
    }

    @DisplayName("backupPvcFile: 日期格式非法目录抛 RuntimeException")
    @Test
    void backupPvcFile_shouldThrow_whenBadDateMonth() throws Exception {
        // 8 位数字但月份非法，如 20261399
        Path badDir = pvcRoot.resolve("20261399");
        Files.createDirectories(badDir);
        assertThatThrownBy(() -> service.backupPvcFile())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("文件夹名不是日期格式");
    }

    @DisplayName("deleteFilePvcTow: 3 个月前文件被删除")
    @Test
    void deleteFilePvcTow_shouldDeleteOldFile() throws Exception {
        Path oldFile = localDir.resolve("old.txt");
        Files.write(oldFile, "old".getBytes());
        Files.setLastModifiedTime(oldFile, java.nio.file.attribute.FileTime.from(
                Instant.now().minus(120, ChronoUnit.DAYS)));
        Path newFile = localDir.resolve("new.txt");
        Files.write(newFile, "new".getBytes());
        service.deleteFilePvcTow();
        assertThat(oldFile).doesNotExist();
        assertThat(newFile).exists();
    }

    @DisplayName("deleteFilePvcTow: 目录不存在抛 RuntimeException")
    @Test
    void deleteFilePvcTow_shouldThrow_whenDirMissing() {
        ReflectionTestUtils.setField(service, "localFileDirRoot", tmpDir.resolve("notexist").toString());
        assertThatThrownBy(() -> service.deleteFilePvcTow())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("临时文件删除失败");
    }

    @DisplayName("deleteFilePvcTow: 空目录不抛异常")
    @Test
    void deleteFilePvcTow_shouldTolerateEmptyDir() {
        // 目录已创建且为空
        service.deleteFilePvcTow();
    }
}
/*
 * AICode End
 */
