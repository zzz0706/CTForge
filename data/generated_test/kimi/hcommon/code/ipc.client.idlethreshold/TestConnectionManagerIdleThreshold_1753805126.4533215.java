package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestConnectionManagerIdleThreshold {

    @Test
    public void customIdleScanThresholdIsLoaded() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY, 1234);

        // 2. Prepare test conditions
        RPC.Builder builder = new RPC.Builder(conf)
                .setProtocol(TestProtocol.class)
                .setInstance(new TestProtocolImpl())
                .setBindAddress("0.0.0.0")
                .setPort(0);
        RPC.Server server = builder.build();

        // 3. Test code: read idleScanThreshold field from ConnectionManager
        Field cmField = Server.class.getDeclaredField("connectionManager");
        cmField.setAccessible(true);
        Object connectionManager = cmField.get(server);

        Field thresholdField = connectionManager.getClass().getDeclaredField("idleScanThreshold");
        thresholdField.setAccessible(true);
        int actualIdleScanThreshold = thresholdField.getInt(connectionManager);

        // 4. Assertions
        assertEquals("idleScanThreshold should be the custom value set in configuration",
                     1234, actualIdleScanThreshold);

        // 5. Code after testing
        server.stop();
    }

    // Dummy protocol interface and implementation for RPC.Server instantiation
    private interface TestProtocol {
        long getProtocolVersion(String protocol, long clientVersion);
    }

    private static class TestProtocolImpl implements TestProtocol {
        @Override
        public long getProtocolVersion(String protocol, long clientVersion) {
            return 1L;
        }
    }
}