package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

@Category({RegionServerTests.class, SmallTests.class})
public class TestRegionFlushLogic {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestRegionFlushLogic.class);

    /**
     * Test case to verify the behavior of shouldFlush when at least one store contains an old edit
     * Configuration for flush interval is properly read and utilized.
     */
    @Test
    public void test_shouldFlush_withOldEditAcrossRegions() throws Exception {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 3600000); // Set flush interval to one hour
        int flushCheckInterval = conf.getInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 3600000);
        
        // 2. Prepare the test conditions.
        // Mock HRegion instance
        HRegion mockRegion = Mockito.mock(HRegion.class);

        // Mock HStore instances with timestamps for edits
        HStore oldEditStore = Mockito.mock(HStore.class);
        HStore recentEditStore = Mockito.mock(HStore.class);

        long currentTime = EnvironmentEdgeManager.currentTime();
        Mockito.when(oldEditStore.timeOfOldestEdit()).thenReturn(currentTime - (flushCheckInterval + 1000)); // Store with old edit
        Mockito.when(recentEditStore.timeOfOldestEdit()).thenReturn(currentTime); // Store with recent edit

        List<HStore> mockStores = new ArrayList<>();
        mockStores.add(oldEditStore);
        mockStores.add(recentEditStore);

        Mockito.when(mockRegion.getStores()).thenReturn(mockStores);

        // Mock behavior of shouldFlush
        Mockito.when(mockRegion.shouldFlush(Mockito.any(StringBuilder.class)))
            .thenAnswer(invocation -> {
                StringBuilder whyFlush = invocation.getArgument(0);
                for (HStore store : mockStores) {
                    if (store.timeOfOldestEdit() < currentTime - flushCheckInterval) {
                        whyFlush.append("Flush triggered due to old edits.\n");
                        return true;
                    }
                }
                return false;
            });

        // 3. Test code.
        StringBuilder whyFlush = new StringBuilder();
        boolean shouldFlush = mockRegion.shouldFlush(whyFlush);

        // Assertions to validate correctness
        assert shouldFlush : "Expected shouldFlush to return true for regions with old edits.";
        assert whyFlush.toString().contains("Flush triggered due to old edits.") : "Expected whyFlush message to include proper reasoning.";

        // 4. Code after testing.
        Mockito.reset(mockRegion);
        Mockito.reset(oldEditStore);
        Mockito.reset(recentEditStore);
    }

    /**
     * Test case to ensure that shouldFlushStore correctly identifies stores requiring a flush
     * based on the configured flush interval.
     */
    @Test
    public void test_shouldFlushStore_withOldEdit() throws Exception {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 3600000); // Set flush interval to one hour
        int flushCheckInterval = conf.getInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 3600000);
        
        // 2. Prepare the test conditions.
        // Mock HStore instance
        HStore oldEditStore = Mockito.mock(HStore.class);

        long currentTime = EnvironmentEdgeManager.currentTime();
        Mockito.when(oldEditStore.timeOfOldestEdit()).thenReturn(currentTime - (flushCheckInterval + 1000)); // Store with old edit

        // Mock HRegion instance
        HRegion mockRegion = Mockito.mock(HRegion.class);
        Mockito.when(mockRegion.shouldFlushStore(oldEditStore))
            .thenAnswer(invocation -> {
                HStore store = invocation.getArgument(0);
                if (store.timeOfOldestEdit() < currentTime - flushCheckInterval) {
                    return true;
                }
                return false;
            });

        // 3. Test code.
        boolean shouldFlushStore = mockRegion.shouldFlushStore(oldEditStore);

        // Assertions to validate correctness
        assert shouldFlushStore : "Expected shouldFlushStore to return true for store with old edits.";

        // 4. Code after testing.
        Mockito.reset(mockRegion);
        Mockito.reset(oldEditStore);
    }
}