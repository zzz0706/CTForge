package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StripeStoreConfig;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactionPolicy;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category({RegionServerTests.class, SmallTests.class})
public class TestStripeCompactionPolicy {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestStripeCompactionPolicy.class);

    private Configuration configuration;
    private StoreConfigInformation storeConfigInfo;
    private CompactionConfiguration compactionConfiguration;
    private StripeStoreConfig stripeStoreConfig;

    @Before
    public void setUp() throws Exception {
        // Initialize Configuration
        configuration = new Configuration();

        // Mock StoreConfigInformation
        storeConfigInfo = mock(StoreConfigInformation.class);
        when(storeConfigInfo.getMemStoreFlushSize()).thenReturn(134217728L); // Default size 128MB

        // Initialize StripeStoreConfig
        stripeStoreConfig = mock(StripeStoreConfig.class); // Add mocked configuration if required

        // Initialize CompactionConfiguration based on mocked StoreConfigInformation
        compactionConfiguration = new CompactionConfiguration(configuration, storeConfigInfo);
    }

    @Test
    public void testStripeCompactionPolicyThrottleLogicLargeCompaction() throws Exception {
        // Instantiate StripeCompactionPolicy using the correct constructor
        StripeCompactionPolicy stripeCompactionPolicy = new StripeCompactionPolicy(configuration, storeConfigInfo, stripeStoreConfig);

        // Retrieve throttle point from CompactionConfiguration
        long throttlePoint = compactionConfiguration.getThrottlePoint();

        // Test logic for compaction size greater than the throttle point
        long largeCompactionSize = throttlePoint + 1024; // Example size larger than throttle point
        assertTrue("Compaction size should be classified as 'large'", stripeCompactionPolicy.throttleCompaction(largeCompactionSize));
    }
}