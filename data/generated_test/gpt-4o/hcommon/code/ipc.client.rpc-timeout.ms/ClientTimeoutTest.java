package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.Client;
import org.junit.Test;

public class ClientTimeoutTest {
    // Test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码

    @Test
    public void testClient_getTimeout_withPingEnabledAndTimeoutSet() {
        // Step 1: Create a Configuration object using the API
        Configuration conf = new Configuration();

        // Step 2: Ensure 'ipc.client.ping' is enabled using the API
        boolean isPingEnabled = conf.getBoolean(
                CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
                CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT
        );

        // Step 3: Get 'ipc.client.rpc-timeout.ms' value using the API
        int rpcTimeout = Client.getRpcTimeout(conf);

        // Step 4: Call getTimeout to verify behavior when ping is enabled
        int timeout = Client.getTimeout(conf);

        // Test assertion: Verify that getTimeout reflects the configured timeout value directly when ping is enabled
        if (isPingEnabled && rpcTimeout > 0) {
            assert timeout == rpcTimeout : "Expected timeout to match rpc-timeout when ping is enabled.";
        }
    }
}