package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestMobUtils {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMobUtils.class);

    @Test
    public void test_createMobCompactorThreadPool_withValidConfiguration() {
        // Step 2: Prepare the test conditions
        Configuration conf = new Configuration();
        
        // Use API to simulate obtaining configuration value
        int maxThreads = conf.getInt(MobConstants.MOB_COMPACTION_THREADS_MAX,
                MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX);

        // Check default value scenario: if not set, the system should use the value from MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX
        assertTrue(maxThreads >= 1);

        // Step 3: Test code
        ExecutorService threadPool = MobUtils.createMobCompactorThreadPool(conf);

        // Validate the thread pool creation
        assertNotNull(threadPool);
        assertTrue(threadPool instanceof ThreadPoolExecutor);

        ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool;

        // Assert the core thread size
        assertEquals(1, executor.getCorePoolSize());

        // Assert the maximum thread size is set as per configuration
        assertEquals(maxThreads, executor.getMaximumPoolSize());

        // Submit tasks to verify behavior under high load
        try {
            for (int i = 0; i < maxThreads; i++) {
                executor.submit(() -> {
                    // Simulate task
                    Threads.sleepWithoutInterrupt(1000);
                });
            }
        } catch (Exception e) {
            fail("Task submission failed: " + e.getMessage());
        }

        // Step 4: Code after testing
        threadPool.shutdown();
    }
}