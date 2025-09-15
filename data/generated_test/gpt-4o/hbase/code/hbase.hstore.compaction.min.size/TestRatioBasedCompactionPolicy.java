package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Category({RegionServerTests.class, SmallTests.class})
public class TestRatioBasedCompactionPolicy {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestRatioBasedCompactionPolicy.class);

    @Test
    public void testApplyCompactionPolicyForOffPeakCompaction() throws Exception {
        // 1. Prepare the test conditions.
        // Use HBase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration config = new Configuration();
        config.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 134217728); // Minimum file size for compaction
        config.setDouble(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_KEY, 1.2); // Compaction ratio
        config.setDouble(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_OFFPEAK_KEY, 2.0); // Off-peak compaction ratio

        // Create mock StoreConfigInformation object.
        StoreConfigInformation storeConfigInfo = mock(StoreConfigInformation.class);

        // Correctly instantiate CompactionConfiguration using Configuration and StoreConfigInformation.
        CompactionConfiguration compactionConfig = new CompactionConfiguration(config, storeConfigInfo);
        RatioBasedCompactionPolicy ratioBasedPolicy = new RatioBasedCompactionPolicy(config, storeConfigInfo);

        // Mock HStoreFile objects with varying file sizes.
        ArrayList<HStoreFile> candidates = new ArrayList<>();
        candidates.add(createMockHStoreFile(50000000)); // File below threshold.
        candidates.add(createMockHStoreFile(200000000)); // File above threshold.

        // 2. Test the compaction policy.
        ArrayList<HStoreFile> result = ratioBasedPolicy.applyCompactionPolicy(candidates, true, true);

        // 3. Verify the results.
        assertNotNull(result); // Ensure result is not null
        assertTrue("Off-peak compaction should influence compaction selection.", result.size() > 0);

        // 4. Code cleanup after testing.
    }

    /**
     * Helper method to create a mock HStoreFile with the specified file size.
     *
     * @param fileSize The desired size of the HStoreFile.
     * @return A mocked HStoreFile object with the specified file size.
     */
    private HStoreFile createMockHStoreFile(long fileSize) throws Exception {
        HStoreFile mockFile = mock(HStoreFile.class);
        StoreFileReader mockReader = mock(StoreFileReader.class);
        when(mockFile.getReader()).thenReturn(mockReader);
        when(mockReader.length()).thenReturn(fileSize);
        return mockFile;
    }
}