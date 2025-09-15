package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestImageServletConfigurations {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetThrottlerWithThrottlingDisabled() {
        // Prepare the test conditions
        Configuration configuration = new Configuration();

        // Test code: Call the method under test
        DataTransferThrottler throttler = ImageServlet.getThrottler(configuration);

        // Code after testing: Verify the expected outcome
        assertNull("Throttler should be null when dfs.image.transfer.bandwidthPerSec is not set or is zero", throttler);
    }
}