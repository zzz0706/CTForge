package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.KeyStoresFactory;
import javax.net.ssl.SSLEngine;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class TestSSLFactory {
    // test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码

    @Test
    public void testSSLFactoryInitializationWithValidConfiguration() throws Exception {
        // 1. 使用API获取配置值，不要硬编码配置值
        Configuration conf = new Configuration();

        // 2. 准备测试条件: Prepare the SSLFactory with mode and configuration.
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // 3. 测试代码: Test the init function
        sslFactory.init();

        // Assertions to ensure SSLFactory initialization
        KeyStoresFactory keyStoresFactory = sslFactory.getKeystoresFactory();
        assertNotNull("KeyStoresFactory should be initialized", keyStoresFactory);

        // Use createSSLEngine() instead of getContext() since getContext() is not a valid method
        SSLEngine sslEngine = sslFactory.createSSLEngine();
        assertNotNull("SSLEngine should be created and initialized", sslEngine);

        // Additional check to ensure valid configuration in sslEngine
        assertTrue("SSLEngine should be in CLIENT mode", sslEngine.getUseClientMode());

        // 4. 测试后的代码: Clean up resources used by SSLFactory
        sslFactory.destroy();
    }
}