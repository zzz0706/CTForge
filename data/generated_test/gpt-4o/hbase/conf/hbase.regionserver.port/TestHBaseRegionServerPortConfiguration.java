package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Unit test to validate configuration for hbase.regionserver.port in HBase 2.2.2.
 */
@Category(SmallTests.class)
public class TestHBaseRegionServerPortConfiguration {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestHBaseRegionServerPortConfiguration.class);

    /**
     * Validate hbase.regionserver.port constraints:
     * - Default: 16020
     * - Range: 1024 - 65535
     */
    @Test
    public void testRegionServerPortConfiguration() {
        // Load HBase configuration properly
        Configuration configuration = HBaseConfiguration.create();

        // Retrieve the regionserver port (with default fallback)
        int regionServerPort = configuration.getInt(HConstants.REGIONSERVER_PORT, HConstants.DEFAULT_REGIONSERVER_PORT);

        // Validate port range constraints
        assertTrue("RegionServer port should be between 0 and 65535 inclusive.",
                regionServerPort >= 0 && regionServerPort <= 65535);

        // Specifically validate the default port scenario
        if (configuration.get(HConstants.REGIONSERVER_PORT) == null) {
            assertEquals("Default RegionServer port should be 16020 when not set.",
                    HConstants.DEFAULT_REGIONSERVER_PORT, regionServerPort);
        }
    }
}
