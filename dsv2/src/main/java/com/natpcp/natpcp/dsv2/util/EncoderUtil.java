package com.natpcp.natpcp.dsv2.util;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.Security;

/**
 * @Auther wangl
 * @Date : 2024/7/17  14:17
 */
@Component
@Slf4j
public class EncoderUtil {
    @Value("${jasypt.encryptor.password}")
    private String password;
    public static String key;

    @PostConstruct
    private void init() {
        key = password;
    }

    public static char[] alphabets = {'0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static int toDigit(char ch, int index) {
        int digit = Character.digit(ch, 16);
        if (digit < 0) {
            return 0;
        } else {
            return digit;
        }
    }

    static {
        // 只注册一次，避免每次加解密都重复 addProvider
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    /**
     * 说明：算法为 BC 默认的 SM4（ECB/PKCS5Padding），与 plt-mgmt 存量密文兼容，
     * 因此此处不能擅自改为 CBC/GCM；如需升级需同步做存量数据迁移。
     */
    public static String encrypt(String password) throws Exception {
        SecretKeySpec secretKey = new SecretKeySpec(requireKey(), "SM4");
        Cipher cipher = Cipher.getInstance("SM4", BouncyCastleProvider.PROVIDER_NAME);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] bytes = cipher.doFinal(password.getBytes(StandardCharsets.UTF_8));
        return new String(encode(bytes));
    }

    private static byte[] requireKey() {
        byte[] k = decode(key);
        if (k == null) {
            throw new IllegalStateException("jasypt.encryptor.password 未配置，无法进行 SM4 加解密");
        }
        return k;
    }

    public static char[] encode(byte[] data) {
        int len = data.length;
        char[] out = new char[len << 1];
        int i = 0;

        for (int j = 0; i < len; ++i) {
            out[j] = alphabets[(240 & data[i]) >>> 4];
            j++;
            out[j] = alphabets[15 & data[i]];
            j++;
        }

        return out;
    }

    public static String decrypt(String password) throws Exception {
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("待解密密文为空");
        }
        SecretKeySpec secretKey = new SecretKeySpec(requireKey(), "SM4");
        Cipher cipher = Cipher.getInstance("SM4", BouncyCastleProvider.PROVIDER_NAME);
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] bytes = cipher.doFinal(decode(password));
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] decode(CharSequence encoded) {
        if (null == encoded || encoded.length() == 0) {
            return null;
        } else {
            int len = encoded.length();
            if ((len & 1) != 0) {
                encoded = "0" + encoded;
                len = encoded.length();
            }
            byte[] out = new byte[len >> 1];
            int i = 0;

            for (int j = 0; j < len; ++i) {
                int f = toDigit(encoded.charAt(j), j) << 4;
                ++j;
                f |= toDigit(encoded.charAt(j), j);
                ++j;
                out[i] = (byte) (f & 255);
            }
            return out;
        }
    }
}
