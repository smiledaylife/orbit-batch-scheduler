package com.natpcp.natpcp.dsv2.batch.util;

import com.alibaba.fastjson.JSON;
import com.natpcp.natpcp.dsv2.batch.util.JobParamsHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * AICode Begin
 * JobParamsHelper 单元测试：Job 参数 JSON 序列化 / 反序列化工具。
 */
@DisplayName("JobParamsHelper 测试")
class JobParamsHelperTest {

    @DisplayName("getParams: JobParameters 为 null 时返回空 Map")
    @Test
    void getParams_shouldReturnEmptyMap_whenJobParametersIsNull() {
        assertThat(JobParamsHelper.getParams(null)).isEmpty();
    }

    @DisplayName("getParams: jobParams 键缺失时返回空 Map")
    @Test
    void getParams_shouldReturnEmptyMap_whenKeyMissing() {
        JobParameters jp = new JobParametersBuilder().addString("other", "x").toJobParameters();
        assertThat(JobParamsHelper.getParams(jp)).isEmpty();
    }

    @DisplayName("getParams: jobParams 为空白字符串时返回空 Map")
    @Test
    void getParams_shouldReturnEmptyMap_whenJsonBlank() {
        JobParameters jp = new JobParametersBuilder().addString(JobParamsHelper.KEY_JOB_PARAMS, "  ").toJobParameters();
        assertThat(JobParamsHelper.getParams(jp)).isEmpty();
    }

    @DisplayName("getParams: 合法 JSON 解析为 Map")
    @Test
    void getParams_shouldParseJson_whenValid() {
        Map<String, Object> origin = new HashMap<>();
        origin.put("name", "tableA");
        origin.put("count", 3);
        JobParameters jp = new JobParametersBuilder()
                .addString(JobParamsHelper.KEY_JOB_PARAMS, JSON.toJSONString(origin))
                .toJobParameters();
        Map<String, Object> result = JobParamsHelper.getParams(jp);
        assertThat(result).containsEntry("name", "tableA").containsEntry("count", 3);
    }

    @DisplayName("getParams: JSON 中值为 null 时返回空 Map")
    @Test
    void getParams_shouldReturnEmptyMap_whenJsonIsNullLiteral() {
        JobParameters jp = new JobParametersBuilder()
                .addString(JobParamsHelper.KEY_JOB_PARAMS, "null")
                .toJobParameters();
        assertThat(JobParamsHelper.getParams(jp)).isEmpty();
    }

    @DisplayName("getParams: 非法 JSON 捕获异常返回空 Map")
    @Test
    void getParams_shouldReturnEmptyMap_whenInvalidJson() {
        JobParameters jp = new JobParametersBuilder()
                .addString(JobParamsHelper.KEY_JOB_PARAMS, "{not-json!!")
                .toJobParameters();
        assertThat(JobParamsHelper.getParams(jp)).isEmpty();
    }

    @DisplayName("getParamsMap: null 入参返回空 Map")
    @Test
    void getParamsMap_shouldReturnEmptyMap_whenNull() {
        assertThat(JobParamsHelper.getParamsMap(null)).isEmpty();
    }

    @DisplayName("getParamsMap: 空 Map 返回空 Map")
    @Test
    void getParamsMap_shouldReturnEmptyMap_whenEmpty() {
        assertThat(JobParamsHelper.getParamsMap(new HashMap<>())).isEmpty();
    }

    @DisplayName("getParamsMap: 缺 jobParams 键返回空 Map")
    @Test
    void getParamsMap_shouldReturnEmptyMap_whenKeyMissing() {
        Map<String, Object> params = new HashMap<>();
        params.put("a", "b");
        assertThat(JobParamsHelper.getParamsMap(params)).isEmpty();
    }

    @DisplayName("getParamsMap: jobParams 值为空白返回空 Map")
    @Test
    void getParamsMap_shouldReturnEmptyMap_whenBlankValue() {
        Map<String, Object> params = new HashMap<>();
        params.put(JobParamsHelper.KEY_JOB_PARAMS, "  ");
        assertThat(JobParamsHelper.getParamsMap(params)).isEmpty();
    }

    @DisplayName("getParamsMap: 合法 JSON 字符串值解析为 Map")
    @Test
    void getParamsMap_shouldParse_whenValidJsonString() {
        Map<String, Object> params = new HashMap<>();
        params.put(JobParamsHelper.KEY_JOB_PARAMS, "{\"k\":\"v\"}");
        assertThat(JobParamsHelper.getParamsMap(params)).containsEntry("k", "v");
    }

    @DisplayName("getParamsMap: 非字符串值（Map.toString 非 JSON）解析失败返回空 Map")
    @Test
    void getParamsMap_shouldReturnEmptyMap_whenValueIsMap() {
        Map<String, Object> inner = new HashMap<>();
        inner.put("x", 1);
        Map<String, Object> params = new HashMap<>();
        params.put(JobParamsHelper.KEY_JOB_PARAMS, inner);
        // Map.toString() = {x=1}，不是合法 JSON，按工具设计应返回空 Map
        assertThat(JobParamsHelper.getParamsMap(params)).isEmpty();
    }

    @DisplayName("getParamsMap: 非法 JSON 返回空 Map")
    @Test
    void getParamsMap_shouldReturnEmptyMap_whenInvalidJson() {
        Map<String, Object> params = new HashMap<>();
        params.put(JobParamsHelper.KEY_JOB_PARAMS, "bad json");
        assertThat(JobParamsHelper.getParamsMap(params)).isEmpty();
    }

    @DisplayName("toJson: null 入参返回 {}")
    @Test
    void toJson_shouldReturnEmptyJson_whenNull() {
        assertThat(JobParamsHelper.toJson(null)).isEqualTo("{}");
    }

    @DisplayName("toJson: 空 Map 返回 {}")
    @Test
    void toJson_shouldReturnEmptyJson_whenEmpty() {
        assertThat(JobParamsHelper.toJson(new HashMap<>())).isEqualTo("{}");
    }

    @DisplayName("toJson: 有值 Map 序列化为 JSON")
    @Test
    void toJson_shouldSerialize_whenHasValues() {
        Map<String, Object> params = new HashMap<>();
        params.put("name", "t");
        assertThat(JobParamsHelper.toJson(params)).isEqualTo("{\"name\":\"t\"}");
    }
}
/*
 * AICode End
 */
