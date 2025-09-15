package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests to validate the configuration for
 * `ha.failover-controller.graceful-fence.connection.retries`.
 */
public class FailoverControllerConfigValidationTest {

    /**
     * Test case to check the validity of the configuration:
     * ha.failover-controller.graceful-fence.connection.retries.
     */
    @Test
    public void testGracefulFenceConnectionRetriesConfig() {
        Configuration conf = new Configuration();

        // Step 1: Read configuration property for graceful fence connection retries.
        String configKey = CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES;
        int configuredValue = conf.getInt(configKey, CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT);

        // Step 2: Validate the configuration value against expected constraints.
        /*
         * Constraints for `ha.failover-controller.graceful-fence.connection.retries`:
         * 1. Should be a positive non-zero integer (connection retries must make sense).
         * 2. Working assumption that connection retries should be reasonable within failover limits.
         */
        Assert.assertTrue(
            "Configuration " + configKey + " must be a positive non-zero integer",
            configuredValue > 0
        );

        // Additional validation can be added per specific failover retry policies or other dependencies if discovered.
    }
}