package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConfigurationValidationTest {

    /**
     * This test validates the configuration property `ipc.maximum.response.length`
     * by checking if the value satisfies its defined constraints.
     */
    @Test
    public void testIpcMaximumResponseLengthConfiguration() {
        Configuration conf = new Configuration();

        // Read the configuration value
        int maxResponseLength = conf.getInt(
                CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
                CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

        // Step 1: Validate constraints
        // Constraint: Value must be >= 0 or it must be 0 to disable
        assertTrue(
            "ipc.maximum.response.length must be >= 0 or 0 to disable.",
            maxResponseLength >= 0
        );
    }
}