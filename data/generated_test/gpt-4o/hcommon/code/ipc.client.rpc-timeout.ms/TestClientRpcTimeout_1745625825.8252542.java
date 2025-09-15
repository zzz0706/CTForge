package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestClientRpcTimeout {

    // Prepare the input conditions for unit testing.
    @Test
    public void testClient_getRpcTimeout_withUnsetTimeout() {
        // Create a new Configuration object with no value set for 'ipc.client.rpc-timeout.ms'.
        Configuration configuration = new Configuration();

        // Invoke Client.getRpcTimeout using the uninitialized object.
        int rpcTimeout = Client.getRpcTimeout(configuration);

        // Assert that the return value matches the default timeout or 0 if the fetched value is below 0.
        assertTrue("RPC timeout should be non-negative", rpcTimeout >= 0);
    }
}