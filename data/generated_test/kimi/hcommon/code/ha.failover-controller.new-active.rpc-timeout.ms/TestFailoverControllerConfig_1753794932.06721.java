package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestFailoverControllerConfig {

    @Test
    public void defaultRpcTimeoutIsUsedWhenNoCustomValueIsSet() {
        // 1. Create a brand-new Configuration that does NOT load any default XML
        //    (core-default.xml / core-site.xml) so we are sure the value is
        //    not overridden anywhere.
        Configuration conf = new Configuration(false);

        // 2. Obtain the value that the runtime would resolve for the key
        int expectedRpcTimeout = conf.getInt(
                CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY,
                CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT);

        // 3. Exercise the method under test
        int actualRpcTimeout = FailoverController.getRpcTimeoutToNewActive(conf);

        // 4. Ensure the method really uses the default value when no custom
        //    configuration is present
        assertEquals("Default RPC timeout should be "
                     + CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT
                     + " ms",
                     expectedRpcTimeout, actualRpcTimeout);
    }
}