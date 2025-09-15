package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import javax.net.SocketFactory;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class TestSocketIOWithTimeout {
    // test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码

    @Test
    public void testGetDefaultSocketFactory_withFallbackMechanism() {
        // 1. Create a Configuration object
        Configuration conf = new Configuration();

        // 2. Prepare test conditions: Ensure no explicit 'hadoop.rpc.socket.factory.class.default' key in configuration
        conf.unset("hadoop.rpc.socket.factory.class.default");

        // 3. Invoke the API under test
        SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(conf);

        // 4. Assert the expected behavior
        assertNotNull(socketFactory);
        assertTrue("Expected StandardSocketFactory but got: " + socketFactory.getClass().getName(),
                socketFactory instanceof org.apache.hadoop.net.StandardSocketFactory);
    }
}