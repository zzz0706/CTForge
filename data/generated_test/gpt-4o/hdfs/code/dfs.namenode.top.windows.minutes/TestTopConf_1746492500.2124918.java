package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions by creating a Configuration object.
    // 3. Instantiate the TopConf class using the Configuration object and assert that exceptions are thrown as expected.
    // 4. Code after testing should clean up resources, if necessary.
    public void testInvalidNonNumericValues() {
        // Step 1: Use the Hadoop configuration API correctly and set the relevant key
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "abc,5,25");
        
        // Step 2: Prepare the test and ensure proper exception handling occurs
        try {
            // Attempt to instantiate the TopConf class with invalid configuration values
            TopConf topConf = new TopConf(conf);
        } catch (NumberFormatException e) {
            // Assert that the exception message matches expectations for non-numeric input
            assert e.getMessage() != null && e.getMessage().contains("For input string: \"abc\"");
        } catch (IllegalArgumentException e) {
            // Assert that the exception message matches expectations for invalid input
            assert e.getMessage() != null && e.getMessage().contains("Invalid configuration value");
        }
    }
}