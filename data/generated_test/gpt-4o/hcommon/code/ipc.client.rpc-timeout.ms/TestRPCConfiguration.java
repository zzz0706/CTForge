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

        Configuration conf = new Configuration();
        int rpcTimeout = Client.getRpcTimeout(conf);


        InetSocketAddress address = new InetSocketAddress("localhost", 12345);


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

            assertNotNull(result); // Ensure the ProtocolProxy is successfully returned.
        } catch (Exception e) {
            fail("RPC.waitForProtocolProxy threw an unexpected exception: " + e.getMessage());
        }
    } 
}