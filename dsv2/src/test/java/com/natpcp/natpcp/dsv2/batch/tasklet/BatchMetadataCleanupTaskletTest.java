/*
 * AICode Begin
 * BatchMetadataCleanupTasklet 单元测试：表前缀必须生效，禁止落到无前缀的 BATCH_* 生产旧表。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("BatchMetadataCleanupTasklet 测试")
class BatchMetadataCleanupTaskletTest {

    @Test
    @DisplayName("所有 DELETE 语句均使用配置的表前缀，且不出现无前缀的 BATCH_ 表名")
    void execute_shouldUseConfiguredTablePrefix() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), any(Timestamp.class))).thenReturn(1);
        when(jdbc.update(anyString())).thenReturn(1);

        BatchMetadataCleanupTasklet tasklet = new BatchMetadataCleanupTasklet(jdbc, "DSV2_BATCH_", 180);
        RepeatStatus status = tasklet.execute(null, null);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);

        ArgumentCaptor<String> withTs = ArgumentCaptor.forClass(String.class);
        verify(jdbc, times(5)).update(withTs.capture(), any(Timestamp.class));
        ArgumentCaptor<String> noArg = ArgumentCaptor.forClass(String.class);
        verify(jdbc, times(1)).update(noArg.capture());

        List<String> sqls = new java.util.ArrayList<>(withTs.getAllValues());
        sqls.addAll(noArg.getAllValues());
        assertThat(sqls).hasSize(6);
        for (String sql : sqls) {
            assertThat(sql).contains("DSV2_BATCH_");
            // 任何 "BATCH_" 前都必须紧跟 "DSV2_"（即不允许裸 BATCH_* 表名）
            assertThat(sql.replace("DSV2_BATCH_", "")).doesNotContain("BATCH_");
        }
    }

    @Test
    @DisplayName("非法表前缀 / 非正保留天数直接拒绝")
    void constructor_shouldRejectInvalidArguments() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        assertThatThrownBy(() -> new BatchMetadataCleanupTasklet(jdbc, "BATCH_; DROP TABLE x;--", 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BatchMetadataCleanupTasklet(jdbc, null, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BatchMetadataCleanupTasklet(jdbc, "DSV2_BATCH_", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
/*
 * AICode End
 */
