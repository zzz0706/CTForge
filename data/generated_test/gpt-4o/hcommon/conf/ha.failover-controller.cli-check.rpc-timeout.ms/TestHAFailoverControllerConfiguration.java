package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestHAFailoverControllerConfiguration {

    @Test
    public void testHAFailoverControllerCliCheckRpcTimeoutMs() {
        Configuration conf = new Configuration();

        // Step 1: Read the configuration value
        int rpcTimeout = conf.getInt(
                CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
                CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);

        // Step 2: Validate the configuration value against the constraints
        // Constraint: The value must be a valid integer and non-negative, as it represents a timeout in milliseconds
        Assert.assertTrue(
                "Configuration value for ha.failover-controller.cli-check.rpc-timeout.ms should be non-negative",
                rpcTimeout >= 0
        );

        // Additional validation (if applicable based on dependency/usage in the context)
        // For this specific key, there are no other dependencies mentioned in the provided context.
    }
}