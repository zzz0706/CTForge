package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.fail;

public class TestSSLServerConfiguration {

    /**
     * Test to validate the configuration `hadoop.ssl.server.conf`
     * Ensures the configuration satisfies its constraints and dependencies.
     */
    @Test
    public void testHadoopSslServerConf() {
        // 1. 使用API获取配置值，不要硬编码配置值
        Configuration conf = new Configuration(false);
        
        // 2. 准备测试条件
        String testResourcePath = "target/test-classes"; // Ensure the file is created in the test classes directory
        File resourceDir = new File(testResourcePath);
        if (!resourceDir.exists() && !resourceDir.mkdirs()) {
            fail("Failed to create test resources directory: " + testResourcePath);
        }
        
        File dummySslServerConfig = new File(resourceDir, "ssl-server.xml");
        if (!dummySslServerConfig.exists()) {
            try {
                if (!dummySslServerConfig.createNewFile()) {
                    fail("Failed to create dummy ssl-server.xml for testing.");
                }
            } catch (IOException e) {
                fail("Unable to create the required resource file: ssl-server.xml. Error: " + e.getMessage());
            }
        }

        // Set SSL server configuration key to point to the file
        conf.set(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");

        // 3. 测试代码
        String sslServerConfValue = conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");
        if (sslServerConfValue == null || sslServerConfValue.isEmpty()) {
            fail("The configuration `hadoop.ssl.server.conf` must not be empty");
        }

        URL resourceURL = getClass().getClassLoader().getResource(sslServerConfValue);
        if (resourceURL == null) {
            fail("The configuration `hadoop.ssl.server.conf` does not point to an existing file in the classpath: " + sslServerConfValue);
        }

        // 4. 测试后的代码
        File sslServerFile = new File(resourceURL.getFile());
        if (!sslServerFile.exists()) {
            fail("The file exists as a resource but could not be resolved properly: " + sslServerFile.getAbsolutePath());
        }
    }
}