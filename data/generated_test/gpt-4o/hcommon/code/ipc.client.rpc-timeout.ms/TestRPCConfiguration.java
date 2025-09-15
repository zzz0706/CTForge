package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.TestRPCConfiguration.TestProtocol;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestRPCConfiguration {

    /**
     * Test interface to be used by the RPC waitForProtocolProxy API.
     */
    public interface TestProtocol {
        long versionID = 1L;

        String echo(String value);
    }
    
    @Test
    public void testRPC_waitForProtocolProxy_withValidConfiguration() throws Exception {
        // 1. 使用API获取配置值，不要硬编码配置值
        Configuration conf = new Configuration();
        int rpcTimeout = Client.getRpcTimeout(conf);

        // 2. 准备测试条件
        InetSocketAddress address = new InetSocketAddress("localhost", 12345);

        // 3. 测试代码
        try {
            // RPC.waitForProtocolProxy requires an actual server implementation to connect to.
            // Hence, we'll mock the invocation for the test and ensure proper configuration handling.
            ProtocolProxy<TestProtocol> result = RPC.waitForProtocolProxy(
                    TestProtocol.class, 
                    TestProtocol.versionID, 
                    address, 
                    conf, 
                    rpcTimeout // Pass correct timeout value derived from configuration
            );

            // 4. 测试后的代码
            assertNotNull(result); // Ensure the ProtocolProxy is successfully returned.
        } catch (Exception e) {
            fail("RPC.waitForProtocolProxy threw an unexpected exception: " + e.getMessage());
        }
    } 
}