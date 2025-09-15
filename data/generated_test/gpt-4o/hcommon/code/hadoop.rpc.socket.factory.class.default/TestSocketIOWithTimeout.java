package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import javax.net.SocketFactory;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class TestSocketIOWithTimeout {
    // test code

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