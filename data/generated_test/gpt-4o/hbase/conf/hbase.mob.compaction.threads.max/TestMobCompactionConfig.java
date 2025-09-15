package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for MOB compaction configuration.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestMobCompactionConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMobCompactionConfig.class);

    /**
     * Test to validate the `hbase.mob.compaction.threads.max` configuration.
     */
    @Test
    public void testMobCompactionThreadsMaxConfiguration() {
        // 1. Use HBaseConfiguration from the proper package to create the Configuration instance
        Configuration conf = HBaseConfiguration.create();

        // 2. Retrieve the configuration value using the key from MobConstants
        String configKey = MobConstants.MOB_COMPACTION_THREADS_MAX;
        int defaultValue = MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX;

        // Fetch the configuration value from the Configuration instance
        int value = conf.getInt(configKey, defaultValue);

        // 3. Evaluate test conditions
        // Validate that the configuration value is positive
        assertTrue("Configuration " + configKey + " must be a positive integer.", value > 0);

        // Optionally, ensure the value is within a reasonable range
        final int maxReasonableThreads = 100;
        assertTrue("Configuration " + configKey + " exceeds the reasonable upper limit of " + maxReasonableThreads + " threads.",
                value <= maxReasonableThreads);

        // 4. Test completed successfully
    }
}