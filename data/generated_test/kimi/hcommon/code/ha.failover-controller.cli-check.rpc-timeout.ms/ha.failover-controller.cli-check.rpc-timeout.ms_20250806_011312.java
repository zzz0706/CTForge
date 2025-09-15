package org.apache.hadoop.ha;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HAAdminZeroTimeoutCompleteTest {

    private HAAdmin admin;
    private Configuration conf;
    private ByteArrayOutputStream outContent;
    private ByteArrayOutputStream errContent;
    private PrintStream originalOut;
    private PrintStream originalErr;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        // Use the Hadoop 2.8.5 API to set the timeout to 0
        conf.setInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY, 0);

        // Capture stdout / stderr
        outContent = new ByteArrayOutputStream();
        errContent = new ByteArrayOutputStream();
        originalOut = System.out;
        originalErr = System.err;
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));

        admin = new HAAdmin() {
            @Override
            protected HAServiceTarget resolveTarget(String target) {
                HAServiceTarget mockTarget = mock(HAServiceTarget.class);
                HAServiceProtocol mockProxy = mock(HAServiceProtocol.class);
                try {
                    // Use the actual timeout that will be read from conf
                    when(mockTarget.getProxy(conf, 0)).thenReturn(mockProxy);
                    InetSocketAddress mockAddr = new InetSocketAddress("dummyHost", 12345);
                    when(mockTarget.getAddress()).thenReturn(mockAddr);

                    // Let monitorHealth throw an exception to exercise error path
                    doThrow(new HealthCheckFailedException("forced health failure"))
                            .when(mockProxy).monitorHealth();
                    // Normal getServiceStatus
                    HAServiceStatus status = mock(HAServiceStatus.class);
                    when(status.getState()).thenReturn(HAServiceProtocol.HAServiceState.STANDBY);
                    when(mockProxy.getServiceStatus()).thenReturn(status);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return mockTarget;
            }

            @Override
            protected java.util.Collection<String> getTargetIds(String groupId) {
                // Return a non-empty collection to allow getAllServiceState to proceed
                return java.util.Collections.singletonList("testService");
            }

            @Override
            protected int runCmd(String[] args) throws Exception {
                return super.runCmd(args);
            }
        };
        admin.setConf(conf);
    }

    @After
    public void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    public void testConfigurationPropagation() {
        // Verify the configuration is read correctly
        assertEquals(0, conf.getInt(
                CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
                CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT));
    }

    @Test
    public void testCheckHealthWithZeroTimeout() throws Exception {
        int exitCode = admin.run(new String[]{"-checkHealth", "testService"});
        assertEquals(-1, exitCode);
        assertTrue(errContent.toString().contains("Health check failed"));
    }

    @Test
    public void testGetServiceStateWithZeroTimeout() throws Exception {
        outContent.reset();
        errContent.reset();
        int exitCode = admin.run(new String[]{"-getServiceState", "testService"});
        assertEquals(0, exitCode);
        assertTrue(outContent.toString().contains("standby"));
    }

    @Test
    public void testGetAllServiceStateWithZeroTimeout() throws Exception {
        outContent.reset();
        errContent.reset();
        int exitCode = admin.run(new String[]{"-getAllServiceState"});
        assertEquals(0, exitCode);
        assertTrue(outContent.toString().contains("dummyHost:12345"));
    }

    @Test
    public void testRunCmdWithInvalidCommand() throws Exception {
        int exitCode = admin.run(new String[]{"-invalidCommand"});
        assertEquals(-1, exitCode);
        assertTrue(errContent.toString().contains("Unknown command"));
    }

    @Test
    public void testRunCmdWithNoArgs() throws Exception {
        int exitCode = admin.run(new String[]{});
        // HAAdmin prints usage and exits with 0 when no args are provided
        assertEquals(0, exitCode);
    }
}