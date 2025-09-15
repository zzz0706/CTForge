package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestMobUtils {

    @ClassRule // HBaseClassTestRule ensures consistent test behavior across environments.
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestMobUtils.class);

    /**
     * Test case: test_createMobCompactorThreadPool_withZeroThreads
     * Objective: Verify that when `hbase.mob.compaction.threads.max` is set to 0 in the configuration,
     * the `createMobCompactorThreadPool` method defaults maxThreads to 1.
     */
    @Test
    public void testCreateMobCompactorThreadPoolWithZeroThreads() {
        // Step 1: Prepare the test configuration object.
        Configuration conf = new Configuration();
        // Using HBase 2.2.2 API correctly to set the configuration value.
        conf.setInt(MobConstants.MOB_COMPACTION_THREADS_MAX, 0); // Setting the max threads to 0.

        // Step 2: Test method execution: create a thread pool using the tested method.
        ExecutorService threadPool = MobUtils.createMobCompactorThreadPool(conf);

        // Step 3: Verify the properties of the resulting thread pool.
        // Verify that the core pool size is set correctly (should default to 1).
        assertEquals(1, ((ThreadPoolExecutor) threadPool).getCorePoolSize());
        // Verify that the maximum pool size is set correctly (should default to 1 due to zero configuration).
        assertEquals(1, ((ThreadPoolExecutor) threadPool).getMaximumPoolSize());

        // Step 4: Cleanup after test (shutdown thread pool).
        threadPool.shutdown();
    }

}