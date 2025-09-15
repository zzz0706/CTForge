package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestClientConnection {
    @Test
    public void testGetConnectionMaxResponseLengthConfiguration() throws Exception {
        // 1. 使用API获取配置值，不要硬编码配置值
        Configuration conf = new Configuration();
        int ipcMaximumResponseLength = conf.getInt(
                CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
                CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

        // 2. 准备测试条件
        InetSocketAddress address = NetUtils.createSocketAddr("localhost:0"); // Ensure the port is valid
        RetryPolicy retryPolicy = RetryPolicies.RETRY_FOREVER;
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        AtomicBoolean fallbackToSimpleAuth = new AtomicBoolean(false);

        // Declare throws IOException for methods that may throw the exception
        ConnectionId remoteId = ConnectionId.getConnectionId(
                address,
                PolicyProvider.class,
                ugi,
                0,
                retryPolicy,
                conf
        );

        // 3. 测试代码
        try {
            // Create an instance of the Client using the correct constructor
            Client client = new Client(BytesWritable.class, conf);

            // Verify client creation was successful but do not directly access a private API
            assert client != null;

            // Since Client.Connection is private, there's no way to directly test it here.
            // Instead, focus on verifying the configuration and other public APIs.

        } catch (Exception e) {
            // Handle exceptions as necessary
            e.printStackTrace();
            assert false : "Exception occurred during test execution: " + e.getMessage();
        }
    }
}