/*
 * AICode Begin
 * ConnectionInfoValidator 单元测试。
 */
package com.natpcp.natpcp.dsv2.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ConnectionInfoValidator 测试")
class ConnectionInfoValidatorTest {

    @Test
    void jdbcUrl_shouldAcceptPlainPostgresUrl() {
        assertThat(ConnectionInfoValidator.requireSafeJdbcUrl(" jdbc:postgresql://10.0.0.1:5432/db?ssl=false "))
                .isEqualTo("jdbc:postgresql://10.0.0.1:5432/db?ssl=false");
    }

    @Test
    void jdbcUrl_shouldRejectNonPostgresOrDangerous() {
        assertThatThrownBy(() -> ConnectionInfoValidator.requireSafeJdbcUrl(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectionInfoValidator.requireSafeJdbcUrl("jdbc:mysql://h/db"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectionInfoValidator.requireSafeJdbcUrl(
                "jdbc:postgresql://h/db?socketFactory=x.Y"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectionInfoValidator.requireSafeJdbcUrl(
                "jdbc:postgresql://h/db?loggerFile=/tmp/x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectionInfoValidator.requireSafeJdbcUrl("jdbc:postgresql://h/db;x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void path_shouldAcceptAbsoluteAndRejectShellMeta() {
        assertThat(ConnectionInfoValidator.requireSafeAbsolutePath("/etc/rocketmq/conf/broker.conf"))
                .isEqualTo("/etc/rocketmq/conf/broker.conf");
        assertThatThrownBy(() -> ConnectionInfoValidator.requireSafeAbsolutePath("conf/broker.conf"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectionInfoValidator.requireSafeAbsolutePath("/etc/x; rm -rf /"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectionInfoValidator.requireSafeAbsolutePath("/etc/$(id)"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectionInfoValidator.requireSafeAbsolutePath("/a/../b"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void port_shouldValidateRange() {
        assertThat(ConnectionInfoValidator.requirePort("6379")).isEqualTo(6379);
        assertThatThrownBy(() -> ConnectionInfoValidator.requirePort("0")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectionInfoValidator.requirePort("70000")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectionInfoValidator.requirePort("22a")).isInstanceOf(IllegalArgumentException.class);
    }
}
/*
 * AICode End
 */
