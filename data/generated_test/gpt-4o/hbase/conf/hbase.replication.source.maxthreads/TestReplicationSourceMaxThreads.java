package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadFactory;

import static org.junit.Assert.assertTrue; // Proper import for assertions
import static org.junit.Assert.assertFalse; // Proper import for assertions

/**
 * Unit test for verifying the replication source max threads configuration is valid.
 */
@Category(SmallTests.class)
public class TestReplicationSourceMaxThreads {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestReplicationSourceMaxThreads.class);

    /**
     * Test that the configuration value of hbase.replication.source.maxthreads satisfies the constraints and dependencies defined in the source code.
     */
    @Test
    public void testMaxThreadsConfiguration() {
        // Step 1: Set up the environment and configuration
        Configuration conf = new Configuration();

        // Step 2: Retrieve the configuration value using HBase API
        // Fetch the configuration value with proper key and default fallback
        int maxThreadsValue = conf.getInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY,
            HConstants.REPLICATION_SOURCE_MAXTHREADS_DEFAULT);

        // Step 3: Validate the configuration value against constraints
        // Constraint: maxThreadsValue must be greater than zero
        assertTrue("Replication source max threads should be greater than zero", maxThreadsValue > 0);

        // Constraint: maxThreadsValue should not exceed a reasonable upper limit
        assertTrue("Replication source max threads should not exceed acceptable limits", maxThreadsValue <= 10000);

        // Step 4: Simulate related functionality to verify compatibility
        // Example: Create thread pool to validate the configuration
        try {
            Threads.getBoundedCachedThreadPool(maxThreadsValue, 60, TimeUnit.SECONDS, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true);
                    thread.setName("SinkThread-" + thread.getId());
                    return thread;
                }
            });
        } catch (IllegalArgumentException e) {
            assertFalse("Thread pool creation failed due to invalid maxThreads value", true);
        }
    }
}