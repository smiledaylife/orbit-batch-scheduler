package com.natpcp.natpcp.dsv2.util;

import com.natpcp.natpcp.dsv2.util.EncoderUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * AICode Begin
 * EncoderUtil unit test (SM4 encrypt/decrypt round-trip with real BouncyCastle).
 */
@DisplayName("EncoderUtil Test")
class EncoderUtilTest {

    @DisplayName("decode: null / 空字符串 返回 null")
    @Test
    void decode_shouldReturnNull_whenBlank() {
        assertThat(EncoderUtil.decode(null)).isNull();
        assertThat(EncoderUtil.decode("")).isNull();
    }

    @DisplayName("decode: 奇数长度自动补 0 前缀")
    @Test
    void decode_shouldPadZero_whenOddLength() {
        byte[] out = EncoderUtil.decode("1");
        assertThat(out).hasSize(1);
        assertThat(out[0]).isEqualTo((byte) 0x01);
    }

    @DisplayName("encode/decode: 十六进制往返一致")
    @Test
    void encodeDecode_shouldRoundTrip() {
        byte[] data = "hello".getBytes();
        char[] hex = EncoderUtil.encode(data);
        byte[] restored = EncoderUtil.decode(new String(hex));
        assertThat(restored).isEqualTo(data);
    }

    @DisplayName("encode: 输出为大写字母数字十六进制字符")
    @Test
    void encode_shouldProduceHexChars() {
        char[] hex = EncoderUtil.encode(new byte[]{(byte) 0xAB, (byte) 0xCD});
        assertThat(hex).hasSize(4);
        assertThat(hex[0]).isEqualTo('a');
        assertThat(hex[1]).isEqualTo('b');
        assertThat(hex[2]).isEqualTo('c');
        assertThat(hex[3]).isEqualTo('d');
    }

    @DisplayName("encrypt/decrypt: SM4 加解密往返（需设置 key）")
    @Test
    void encryptDecrypt_shouldRoundTrip() throws Exception {
        EncoderUtil.key = "0123456789abcdef0123456789abcdef";
        String plain = "hello@world";
        String encrypted = EncoderUtil.encrypt(plain);
        assertThat(encrypted).isNotEqualTo(plain);
        assertThat(EncoderUtil.decrypt(encrypted)).isEqualTo(plain);
    }

    @DisplayName("encrypt: 同一明文不同 key 密文不同")
    @Test
    void encrypt_shouldDiffer_whenKeyDiffer() throws Exception {
        EncoderUtil.key = "0123456789abcdef0123456789abcdef";
        String c1 = EncoderUtil.encrypt("abc");
        EncoderUtil.key = "fedcba9876543210fedcba9876543210";
        String c2 = EncoderUtil.encrypt("abc");
        assertThat(c1).isNotEqualTo(c2);
    }
}
/*
 * AICode End
 */
