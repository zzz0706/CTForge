package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Field;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertTrue;

/**
 * This test ensures that MobFileCache initializes correctly with valid configuration values
 * for eviction period and cache size, and verifies that eviction tasks are scheduled as expected.
 */
@Category(SmallTests.class)
public class TestMobFileCacheInitialization {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMobFileCacheInitialization.class);

    private MobFileCache mobFileCache;
    private Configuration configuration;

    @Before
    public void setUp() {
        // Prepare the configuration with valid values.
        configuration = new Configuration();
        configuration.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, MobConstants.DEFAULT_MOB_FILE_CACHE_SIZE);
        configuration.setLong(MobConstants.MOB_CACHE_EVICT_PERIOD, MobConstants.DEFAULT_MOB_CACHE_EVICT_PERIOD);
    }

    @Test
    public void testMobFileCacheInitializationWithValidConfiguration() throws NoSuchFieldException, IllegalAccessException {
        // Step 1: Get valid values using API.
        int mobFileMaxCacheSize = configuration.getInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, MobConstants.DEFAULT_MOB_FILE_CACHE_SIZE);
        long evictionPeriod = configuration.getLong(MobConstants.MOB_CACHE_EVICT_PERIOD, MobConstants.DEFAULT_MOB_CACHE_EVICT_PERIOD);

        // Step 2: Initialize MobFileCache with the configuration.
        mobFileCache = new MobFileCache(configuration);

        // Step 3: Verify that MobFileCache schedules the eviction task when the cache is enabled.
        if (mobFileMaxCacheSize > 0) {
            Field scheduleThreadPoolField = MobFileCache.class.getDeclaredField("scheduleThreadPool");
            scheduleThreadPoolField.setAccessible(true);
            ScheduledThreadPoolExecutor scheduleThreadPool = 
                    (ScheduledThreadPoolExecutor) scheduleThreadPoolField.get(mobFileCache);

            assertTrue("Scheduled thread pool should have tasks scheduled for eviction.",
                    scheduleThreadPool.getQueue().size() > 0);
        }
    }

    @After
    public void tearDown() throws Exception {
        // Shutdown the MobFileCache to clean up resources.
        if (mobFileCache != null) {
            mobFileCache.shutdown();
        }
    }
}