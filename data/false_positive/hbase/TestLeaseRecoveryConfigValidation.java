package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.FSHDFSUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests to verify the validity of HBase lease recovery configuration values
 * and their constraints for hbase.lease.recovery.timeout and related settings.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestLeaseRecoveryConfigValidation {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestLeaseRecoveryConfigValidation.class);

    private static Configuration conf;

    @BeforeClass
    public static void setup() {
        // Initialize the configuration before running tests
        conf = new Configuration();
    }

    @Test
    public void testLeaseRecoveryTimeoutConfiguration() {
        try {
            // Obtain the configuration value for hbase.lease.recovery.timeout
            int leaseRecoveryTimeout = conf.getInt("hbase.lease.recovery.timeout", 900000);

            // Constraint: Check that the lease recovery timeout is a positive integer
            assertTrue(
                    "hbase.lease.recovery.timeout should be a positive integer (greater than 0)",
                    leaseRecoveryTimeout > 0
            );
            assertEquals(900000, leaseRecoveryTimeout);
        } catch (Exception e) {
            fail("An unexpected exception occurred when validating hbase.lease.recovery.timeout: " + e.getMessage());
        }
    }

    @Test
    public void testLeaseRecoveryFirstPauseConfiguration() {
        try {
            // Obtain the configuration value for hbase.lease.recovery.first.pause
            int firstPause = conf.getInt("hbase.lease.recovery.first.pause", 4000);

            // Constraint: Check that the first pause is a positive integer
            assertTrue(
                    "hbase.lease.recovery.first.pause should be a positive integer (greater than 0)",
                    firstPause > 0
            );
        } catch (Exception e) {
            fail("An unexpected exception occurred when validating hbase.lease.recovery.first.pause: " + e.getMessage());
        }
    }

    @Test
    public void testLeaseRecoveryDFSTimeoutConfiguration() {
        try {
            // Obtain the configuration value for hbase.lease.recovery.dfs.timeout
            long dfsTimeout = conf.getLong("hbase.lease.recovery.dfs.timeout", 64 * 1000);

            // Constraint: Check that the DFS timeout is a positive integer
            assertTrue(
                    "hbase.lease.recovery.dfs.timeout should be a positive integer (greater than 0)",
                    dfsTimeout > 0
            );
        } catch (Exception e) {
            fail("An unexpected exception occurred when validating hbase.lease.recovery.dfs.timeout: " + e.getMessage());
        }
    }

    @Test
    public void testLeaseRecoveryPauseConfiguration() {
        try {
            // Obtain the configuration value for hbase.lease.recovery.pause
            int recoveryPause = conf.getInt("hbase.lease.recovery.pause", 1000);

            // Constraint: Check that the recovery pause is a positive integer
            assertTrue(
                    "hbase.lease.recovery.pause should be a positive integer (greater than 0)",
                    recoveryPause > 0
            );
        } catch (Exception e) {
            fail("An unexpected exception occurred when validating hbase.lease.recovery.pause: " + e.getMessage());
        }
    }
}