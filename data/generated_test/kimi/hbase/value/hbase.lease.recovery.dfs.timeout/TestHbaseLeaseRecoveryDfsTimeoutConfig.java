package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({MiscTests.class, SmallTests.class})
public class TestHbaseLeaseRecoveryDfsTimeoutConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestHbaseLeaseRecoveryDfsTimeoutConfig.class);

    private static Configuration conf;

    @BeforeClass
    public static void setUpBeforeClass() {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        conf = HBaseConfiguration.create();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        // 4. Code after testing.
        conf = null;
    }

    /**
     * Test that the configured value for hbase.lease.recovery.dfs.timeout
     * is a positive long and larger than the sum of
     * dfs.heartbeat.interval (default 3s) and dfs.client.socket-timeout (default 60s).
     */
    @Test
    public void testHbaseLeaseRecoveryDfsTimeoutValidity() {
        // 3. Test code.
        final String key = "hbase.lease.recovery.dfs.timeout";
        final long defaultVal = 64000L;

        long configured = conf.getLong(key, defaultVal);

        // 1. Must be a positive long
        assertTrue(key + " must be positive, got: " + configured, configured > 0);

        // 2. Must be larger than (dfs.heartbeat.interval + dfs.client.socket-timeout)
        long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3000L);
        long socketTimeout = conf.getLong("dfs.client.socket-timeout", 60000L);
        long requiredMinimum = heartbeatInterval + socketTimeout;

        assertTrue(
            key + " must be greater than dfs.heartbeat.interval + dfs.client.socket-timeout (" +
                requiredMinimum + " ms), but was " + configured + " ms",
            configured >= requiredMinimum
        );
    }
}