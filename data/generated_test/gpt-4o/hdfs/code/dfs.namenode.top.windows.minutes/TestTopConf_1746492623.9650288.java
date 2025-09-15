package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.junit.Test;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testMixedConfigurationValues() {
        // Step 1: Prepare the test conditions.
        // Create a Configuration object and set the dfs.namenode.top.windows.minutes property with mixed valid and invalid values
        Configuration conf = new Configuration();
        String mixedValues = "1,-5,10"; // includes valid and invalid (negative) values
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, mixedValues);

        // Step 2: Test code.
        try {
            // Instantiate TopConf to trigger validation
            new TopConf(conf);
            // Fail the test if no exception is thrown
            assert false : "Expected IllegalArgumentException not thrown for invalid configuration values";
        } catch (IllegalArgumentException e) {
            // Step 3: Verify the behavior when encountering invalid configuration values.
            // Ensure the exception message contains details about invalid negative time periods.
            assert e.getMessage().contains("minimum reporting period is 1 min!")
                    : "Exception message does not indicate the invalid negative value";
        }

        // Step 4: Code after testing.
        // After the exception, verify that no partial initialization of TopConf occurs.
        // As TopConf throws at instantiation, no further state initialization is expected.
    }
}