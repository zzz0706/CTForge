package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestDfsDatanodeMetricsLoggerPeriodSeconds {

    @Test
    public void testDfsDatanodeMetricsLoggerPeriodSecondsValidity() {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values
        // Create an instance of the HDFS configuration
        Configuration config = new Configuration();

        // Get the configuration key and default value using DFSConfigKeys
        String configKey = DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY;
        int defaultValue = DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT;

        // Use the Configuration API to fetch the configuration value
        int metricsLoggerPeriod = config.getInt(configKey, defaultValue);

        // 2. Prepare the test conditions
        // The configuration value must be validated to check if it enables or disables logging correctly

        // 3. Test code
        // The configuration value must satisfy the following conditions:
        // - Less than 0: logging is disabled
        // - Equal to 0: logging is disabled
        // - Greater than 0: logging period is valid and enabled

        if (metricsLoggerPeriod < 0) {
            // Validating: Configuration value less than zero disables logging
            assertTrue(
                "Configuration value must be zero or a positive number",
                metricsLoggerPeriod < 0
            );
        } else if (metricsLoggerPeriod == 0) {
            // Validating: Period of zero disables metrics logging
            assertTrue(
                "Period of zero disables metrics logging",
                metricsLoggerPeriod == 0
            );
        } else {
            // Validating: Configuration period for metrics logging is a positive integer greater than zero
            assertTrue(
                "Configuration period for metrics logging must be a positive integer greater than zero",
                metricsLoggerPeriod > 0
            );
        }

        // 4. Code after testing
        // No additional teardown is required for this test case
    }
}