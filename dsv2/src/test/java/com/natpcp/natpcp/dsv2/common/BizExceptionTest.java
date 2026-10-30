package com.natpcp.natpcp.dsv2.common;

import com.natpcp.natpcp.dsv2.common.BizException;
import com.natpcp.natpcp.dsv2.common.ResponseStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * AICode Begin
 * BizException unit test.
 */
@DisplayName("BizException Test")
class BizExceptionTest {

    private ResponseStatus status(String code, String message) {
        return new ResponseStatus() {
            @Override
            public String getCode() {
                return code;
            }

            @Override
            public String getSubCode() {
                return null;
            }

            @Override
            public String getMessage() {
                return message;
            }

            @Override
            public String getResolutionNo() {
                return null;
            }
        };
    }

    @DisplayName("constructor(responseStatus): message & responseStatus set, errorInfo null")
    @Test
    void constructor_shouldSetStatus_whenOnlyStatus() {
        ResponseStatus st = status("E100", "some error");
        BizException ex = new BizException(st);
        assertThat(ex.getMessage()).isEqualTo("some error");
        assertThat(ex.getResponseStatus()).isSameAs(st);
        assertThat(ex.getErrorInfo()).isNull();
    }

    @DisplayName("constructor(responseStatus): null status 时 message 为默认值")
    @Test
    void constructor_shouldUseDefault_whenNullStatus() {
        BizException ex = new BizException(null);
        assertThat(ex.getMessage()).isEqualTo("BizException");
        assertThat(ex.getResponseStatus()).isNull();
    }

    @DisplayName("constructor(responseStatus, errorInfo): 额外信息成为 cause 消息")
    @Test
    void constructor_shouldWrapErrorInfo_whenProvided() {
        ResponseStatus st = status("E200", "base msg");
        BizException ex = new BizException(st, "detail info");
        assertThat(ex.getMessage()).isEqualTo("base msg");
        assertThat(ex.getErrorInfo()).isEqualTo("detail info");
        assertThat(ex.getCause()).isNotNull();
        assertThat(ex.getCause().getMessage()).isEqualTo("detail info");
    }

    @DisplayName("constructor(responseStatus, errorInfo): null errorInfo 时无 cause")
    @Test
    void constructor_shouldHaveNoCause_whenErrorInfoNull() {
        ResponseStatus st = status("E300", "base msg");
        BizException ex = new BizException(st, (String) null);
        assertThat(ex.getErrorInfo()).isNull();
        assertThat(ex.getCause()).isNull();
    }

    @DisplayName("constructor(responseStatus, throwable): cause 为 throwable")
    @Test
    void constructor_shouldWrapCause_whenThrowable() {
        ResponseStatus st = status("E400", "base msg");
        RuntimeException cause = new RuntimeException("root");
        BizException ex = new BizException(st, cause);
        assertThat(ex.getCause()).isSameAs(cause);
        assertThat(ex.getErrorInfo()).isNull();
    }
}
/*
 * AICode End
 */
