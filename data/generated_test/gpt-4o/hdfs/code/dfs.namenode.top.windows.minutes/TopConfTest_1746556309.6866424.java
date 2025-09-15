package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

public class TopConfTest {

    private Configuration mockConfiguration;

    @Before
    public void setUp() {
        // 1. Prepare the test conditions by instantiating a mock configuration.
        mockConfiguration = new Configuration();
    }

    @Test(expected = NumberFormatException.class)
    // test code
    // 1. Use the hdfs 2.8.5 API correctly to obtain configuration keys, avoiding hardcoding configuration keys or values.
    // 2. Prepare invalid configuration to simulate the error scenario (non-numeric value).
    // 3. Create an instance of TopConf with the invalid configuration.
    // 4. Validate that the NumberFormatException is thrown when the invalid configuration is used.
    public void testPeriodNonNumeric() {
        // Set an invalid non-numeric value for the NNTOP_WINDOWS_MINUTES_KEY configuration
        mockConfiguration.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "invalid_number");

        // Create the instance of TopConf, which should throw the expected exception
        new TopConf(mockConfiguration);
    }
}