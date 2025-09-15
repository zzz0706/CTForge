package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestHAConfiguration {

    /**
     * Test to validate the configuration value for 'ha.failover-controller.new-active.rpc-timeout.ms'.
     */
    @Test
    public void testNewActiveRpcTimeoutMsConfiguration() {
        Configuration conf = new Configuration();

        // Step 1: Retrieve the configuration value
        int rpcTimeout = conf.getInt(
                CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY,
                CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT);

        // Step 2: Validate the retrieved value based on constraints
        // This configuration value must be a positive integer greater than zero.
        Assert.assertTrue(
                "Configuration 'ha.failover-controller.new-active.rpc-timeout.ms' must be greater than 0",
                rpcTimeout > 0
        );
    }
}