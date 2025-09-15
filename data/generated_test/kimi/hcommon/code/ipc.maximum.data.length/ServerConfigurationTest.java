package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ServerConfigurationTest {

    @Test
    public void testCustomMaxDataLengthIsAccepted() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, 8388608);

        // 2. Prepare the test conditions.
        // Create a concrete subclass of Server for testing
        Server server = new Server("localhost", 0, null, 1, 1, 1, conf,
                "testServer", null, null) {
            @Override
            public Writable call(RPC.RpcKind rpcKind, String protocol,
                                 Writable param, long receiveTime) throws Exception {
                return null;
            }
        };

        // 3. Test code.
        // Use reflection to access the private field maxDataLength
        java.lang.reflect.Field maxDataLengthField = Server.class.getDeclaredField("maxDataLength");
        maxDataLengthField.setAccessible(true);
        int actualMaxDataLength = (Integer) maxDataLengthField.get(server);

        int expectedMaxDataLength = conf.getInt(
                CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
                CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);

        // 4. Code after testing.
        assertEquals(expectedMaxDataLength, actualMaxDataLength);
    }
}