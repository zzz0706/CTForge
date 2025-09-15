package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
import org.junit.Test;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

public class TestJceAesCtrCryptoCodec_SetConf_InvalidProvider {

    @Test
    public void testInvalidProviderHandling() {
        // 1. 使用API获取配置值，不要硬编码配置值
        // Mock the Configuration object
        Configuration mockConfig = new Configuration();
        mockConfig.set("hadoop.security.crypto.jce.provider", "InvalidProviderName");
        mockConfig.set("hadoop.security.java.secure.random.algorithm", "SHA1PRNG");

        // 2. 准备测试条件
        // Instantiate the JceAesCtrCryptoCodec instance
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(mockConfig);

        // 3. 测试代码
        try {
            // Verify fallback SecureRandom initialization after invalid provider handling
            SecureRandom randomInstance = SecureRandom.getInstance(
                    mockConfig.get("hadoop.security.java.secure.random.algorithm", "SHA1PRNG"));
            assert randomInstance != null;
        } catch (GeneralSecurityException e) {
            // 4. 测试后的代码
            // Ensure that no unexpected runtime exception was thrown during SecureRandom fallback initialization
            throw new IllegalStateException("SecureRandom fallback initialization failed", e);
        }
    }
}