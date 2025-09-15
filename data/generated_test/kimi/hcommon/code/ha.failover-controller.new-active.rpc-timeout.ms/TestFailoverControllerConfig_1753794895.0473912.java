package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestFailoverControllerConfig {

    @Test
    public void defaultRpcTimeoutIsUsedWhenNoCustomValueIsSet() {
        // 1. Instantiate a fresh Configuration without explicit set calls
        Configuration conf = new Configuration(false); // false => skip loading default resources

        // 2. Compute the expected value dynamically via Configuration
        int expectedRpcTimeout = conf.getInt(
                CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY,
                CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT);

        // 3. Invoke the method under test
        int actualRpcTimeout = FailoverController.getRpcTimeoutToNewActive(conf);

        // 4. Assert the result
        assertEquals("Default RPC timeout should be 60000 ms",
                     expectedRpcTimeout, actualRpcTimeout);
    }
}