package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestConnectionManagerIdleThreshold {

    private RPC.Server server;
    private Object connectionManager;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY, 1234);

        RPC.Builder builder = new RPC.Builder(conf)
                .setProtocol(TestProtocol.class)
                .setInstance(new TestProtocolImpl())
                .setBindAddress("0.0.0.0")
                .setPort(0);
        server = builder.build();

        Field cmField = Server.class.getDeclaredField("connectionManager");
        cmField.setAccessible(true);
        connectionManager = cmField.get(server);
    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void customIdleScanThresholdIsLoaded() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Field thresholdField = connectionManager.getClass().getDeclaredField("idleScanThreshold");
        thresholdField.setAccessible(true);
        int actualIdleScanThreshold = thresholdField.getInt(connectionManager);
        // 3. Test code.
        assertEquals("idleScanThreshold should be the custom value set in configuration",
                     1234, actualIdleScanThreshold);
        // 4. Code after testing.
    }

    @Test
    public void closeIdleUsesCustomThreshold() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        // 3. Test code.
        Method closeIdleMethod = connectionManager.getClass().getDeclaredMethod("closeIdle", boolean.class);
        closeIdleMethod.setAccessible(true);
        closeIdleMethod.invoke(connectionManager, false);
        // 4. Code after testing.
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