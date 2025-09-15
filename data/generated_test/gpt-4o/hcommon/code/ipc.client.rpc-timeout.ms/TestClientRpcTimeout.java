package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Client;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestClientRpcTimeout {

    @Test
    public void testClient_getRpcTimeout_withValidTimeoutConfigured() {
        // Create a Configuration object
        Configuration conf = new Configuration();

        // Fetch the configuration value using API and validate the functional behavior
        int rpcTimeout = Client.getRpcTimeout(conf);

        // Check if the returned timeout value is non-negative
        assertTrue("Fetched RPC timeout should not be negative", rpcTimeout >= 0);
    }
}