package org.apache.hadoop.ha;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.junit.Test;
import org.junit.Before;

import java.io.IOException;

public class HAAdminZeroTimeoutTest {

    private HAAdmin admin;
    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY, 0);
        admin = new HAAdmin() {
            @Override
            protected HAServiceTarget resolveTarget(String target) {
                HAServiceTarget mockTarget = mock(HAServiceTarget.class);
                HAServiceProtocol mockProxy = mock(HAServiceProtocol.class);
                try {
                    when(mockTarget.getProxy(conf, 0)).thenReturn(mockProxy);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return mockTarget;
            }

            @Override
            protected int runCmd(String[] args) throws Exception {
                return super.runCmd(args);
            }
        };
        admin.setConf(conf);
    }

    @Test
    public void testCliRpcTimeoutZeroBranchBehavior() throws Exception {
        // 1. Compute expected value dynamically
        int expectedTimeout = conf.getInt(
                CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
                CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);

        // 2. Ensure timeout is 0 as configured
        assertEquals(0, expectedTimeout);

        // 3. Execute the test command
        int exitCode = admin.run(new String[]{"-checkHealth", "testService"});

        // 4. Validate no exception thrown and exit code is handled
        assertTrue("Exit code should be handled gracefully", exitCode >= 0);
    }
}