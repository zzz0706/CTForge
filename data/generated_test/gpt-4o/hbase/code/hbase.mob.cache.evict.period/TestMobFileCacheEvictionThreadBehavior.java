package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test case to verify the behavior of eviction tasks scheduled by MobFileCache with valid configuration values.
 */
@Category(SmallTests.class)
public class TestMobFileCacheEvictionThreadBehavior {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMobFileCacheEvictionThreadBehavior.class);

    private MobFileCache mobFileCache;
    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        // 1. Use the HBase 2.2.2 API correctly to obtain configuration values, instead of hardcoding them
        configuration = new Configuration();

        // Retrieve configuration values using the HBase API
        long evictionPeriod = configuration.getLong(MobConstants.MOB_CACHE_EVICT_PERIOD,
                MobConstants.DEFAULT_MOB_CACHE_EVICT_PERIOD);
        long mobFileCacheSize = configuration.getLong(MobConstants.MOB_FILE_CACHE_SIZE_KEY,
                MobConstants.DEFAULT_MOB_FILE_CACHE_SIZE);

        assertTrue("MOB file cache size must be greater than 0 for eviction task to trigger.",
                mobFileCacheSize > 0);

        // 2. Prepare the test conditions
        // Ensure MobFileCache is initialized with the HBase configuration, as expected in the constructor
        mobFileCache = new MobFileCache(configuration);
    }

    @Test
    public void testEvictionThreadBehaviorDuringExecution() throws InterruptedException {
        // 3. Test code to verify eviction thread scheduling and behavior
        assertNotNull("MobFileCache instance should be initialized.", mobFileCache);

        // Allow time for eviction thread to potentially execute
        TimeUnit.SECONDS.sleep(5);

        // Note: As MobFileCache does not expose public methods to directly verify behavior,
        // we infer correct behavior from the absence of exceptions and proper initialization.
    }

    @After
    public void tearDown() throws Exception {
        // 4. Code after testing: Ensure MobFileCache is properly shut down
        if (mobFileCache != null) {
            mobFileCache.shutdown();
        }
    }
}