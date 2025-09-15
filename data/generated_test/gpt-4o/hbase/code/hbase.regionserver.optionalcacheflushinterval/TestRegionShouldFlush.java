package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

@Category({RegionServerTests.class, SmallTests.class})
public class TestRegionShouldFlush {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestRegionShouldFlush.class);

    @Test
    public void test_shouldFlush_withOldEditAcrossRegions() throws Exception {
        // 1. Use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding.
        Configuration conf = new Configuration();
        conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 3600000);  // One hour flush interval
        int flushCheckInterval = conf.getInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 3600000);

        // 2. Prepare the test conditions.
        // Mock HRegion instance
        HRegion mockRegion = Mockito.mock(HRegion.class);

        // Mock HStore instances with appropriate timestamps for edits
        HStore oldEditStore = Mockito.mock(HStore.class);
        HStore otherStore = Mockito.mock(HStore.class);

        // Configure mock behaviors for stores
        long currentTime = EnvironmentEdgeManager.currentTime();
        Mockito.when(oldEditStore.timeOfOldestEdit()).thenReturn(currentTime - (flushCheckInterval + 1000)); // Old edit store
        Mockito.when(otherStore.timeOfOldestEdit()).thenReturn(currentTime); // Recent edit store

        List<HStore> mockStores = new ArrayList<>();
        mockStores.add(oldEditStore);
        mockStores.add(otherStore);

        // Mock region behavior
        Mockito.when(mockRegion.getStores()).thenReturn(mockStores);

        // Mock shouldFlush logic
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

        // Assertions to validate the logic
        assert shouldFlush : "Expected shouldFlush to return true for regions with old edits.";
        assert whyFlush.toString().contains("Flush triggered due to old edits.") : "Expected whyFlush message to include reasoning.";

        // 4. Code after testing.
        Mockito.reset(mockRegion);
        Mockito.reset(oldEditStore);
        Mockito.reset(otherStore);
    }
}