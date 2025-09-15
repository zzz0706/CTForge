package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class IpcClientRpcTimeoutTest {

    /**
     * Test to validate the configuration "ipc.client.rpc-timeout.ms".
     * The test checks if the configuration value satisfies the constraints and dependencies.
     */
    @Test
    public void testRpcTimeoutConfigurations() {
        Configuration conf = new Configuration();

        // Fetch the rpc-timeout from configuration
        int rpcTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
                CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);

        // Constraint 1: The value must not be negative (minimum is 0).
        assertTrue("rpc-timeout must not be negative", rpcTimeout >= 0);

        // Constraint 2: Validate dependency on ipc.client.ping and ipc.ping.interval.
        boolean ipcClientPingEnabled = conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
                CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT);
        int pingInterval = conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
                CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);

        if (!ipcClientPingEnabled) {
            // If ipc.client.ping is not enabled, timeout must not be greater than pingInterval.
            assertTrue("rpc-timeout must not be greater than ipc.ping.interval when ipc.client.ping is disabled",
                    rpcTimeout <= pingInterval);
        } else if (rpcTimeout > pingInterval) {
            // If ipc.client.ping is enabled and timeout is greater than pingInterval,
            // rpc-timeout must be rounded up to multiple of pingInterval.
            int effectiveRpcTimeout = ((rpcTimeout + pingInterval - 1) / pingInterval) * pingInterval;
            assertEquals("Effective rpc-timeout must be a rounded-up multiple of ipc.ping.interval",
                    rpcTimeout, effectiveRpcTimeout);
        }

        // Additional validation: Default values must align with the constraints.
        int defaultTimeout = CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT;
        assertTrue("Default rpc-timeout must not be negative", defaultTimeout >= 0);

        // Dependency-related checks:
        boolean defaultClientPing = CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT;
        int defaultPingInterval = CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT;

        if (!defaultClientPing) {
            assertTrue("Default rpc-timeout must not be greater than default ping interval when ipc.client.ping is disabled",
                    defaultTimeout <= defaultPingInterval);
        } else if (defaultTimeout > defaultPingInterval) {
            int effectiveRpcTimeout = ((defaultTimeout + defaultPingInterval - 1) / defaultPingInterval) * defaultPingInterval;
            assertEquals("Effective default rpc-timeout must be rounded-up multiple of default ping interval",
                    defaultTimeout, effectiveRpcTimeout);
        }
    }
}