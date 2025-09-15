package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInvalidNonNumericValues() {
        // Prepare the test conditions by creating a Configuration object
        Configuration conf = new Configuration();
        
        // Using the API, set the configuration value to 'abc,5,25'
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "abc,5,25");

        try {
            // Instantiate the TopConf class with the prepared configuration
            TopConf topConf = new TopConf(conf);
        } catch (NumberFormatException e) {
            // Assert that the exception is thrown as expected
            assert e.getMessage() != null && e.getMessage().contains("For input string: \"abc\"");
        } catch (IllegalArgumentException e) {
            // Assert that the exception is thrown as expected
            assert e.getMessage() != null && e.getMessage().contains("Invalid configuration value");
        }
    }
}