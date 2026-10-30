package com.natpcp.natpcp.dsv2.util;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
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

    public static String encrypt(String password) throws Exception {
        Security.addProvider(new BouncyCastleProvider());
        SecretKeySpec secretKey = new SecretKeySpec(decode(key), "SM4");
        Cipher cipher = Cipher.getInstance("SM4", BouncyCastleProvider.PROVIDER_NAME);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] bytes = cipher.doFinal(password.getBytes());
        return new String(encode(bytes));

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
        Security.addProvider(new BouncyCastleProvider());
        SecretKeySpec secretKey = new SecretKeySpec(decode(key), "SM4");
        Cipher cipher = Cipher.getInstance("SM4", BouncyCastleProvider.PROVIDER_NAME);
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] bytes = cipher.doFinal(decode(password));
        return new String(bytes);
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
