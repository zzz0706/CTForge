package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.StripeStoreConfig;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;

@Category(SmallTests.class) // Correctly categorizing the test
public class TestStripeCompactionPolicySmallCompaction {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestStripeCompactionPolicySmallCompaction.class);

    @Test
    public void testThrottleCompactionStripePolicySmallCompaction() {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration configuration = new Configuration();
        configuration.setLong("hbase.hstore.compaction.throttle.size", 2684354560L); // Correct configuration key

        // 2. Prepare the test conditions.

        // Create a mock StoreConfigInformation implementation with correct abstract methods.
        StoreConfigInformation storeConfigInfo = new StoreConfigInformation() {
            @Override
            public long getMemStoreFlushSize() {
                return 128 * 1024 * 1024L; // 128 MB
            }

            @Override
            public long getBlockingFileCount() {
                return 10L; // Mocked value for store configuration testing.
            }

            @Override
            public long getCompactionCheckMultiplier() {
                return 1L; // Mocked value
            }

            @Override
            public long getStoreFileTtl() {
                return 60000L; // Mocked value for store file TTL
            }
        };

        // Instantiate StripeStoreConfig with necessary parameters.
        StripeStoreConfig stripeStoreConfig = new StripeStoreConfig(configuration, storeConfigInfo);

        // Instantiate CompactionConfiguration based on configuration and store config information.
        CompactionConfiguration compactionConfiguration = new CompactionConfiguration(configuration, storeConfigInfo);

        // Create an instance of StripeCompactionPolicy.
        StripeCompactionPolicy stripeCompactionPolicy = new StripeCompactionPolicy(configuration, storeConfigInfo, stripeStoreConfig);

        // 3. Test code.

        // Retrieve throttle point using the correct configuration key.
        long throttlePoint = configuration.getLong("hbase.hstore.compaction.throttle.size", 2684354560L);
        long smallCompactionSize = throttlePoint - 1; // A size slightly smaller than the throttle point.

        // Assert that the throttleCompaction method returns false for sizes below the throttle point.
        assertFalse("Expected small compaction classification for compaction size <= throttlePoint",
                stripeCompactionPolicy.throttleCompaction(smallCompactionSize));
    }
}