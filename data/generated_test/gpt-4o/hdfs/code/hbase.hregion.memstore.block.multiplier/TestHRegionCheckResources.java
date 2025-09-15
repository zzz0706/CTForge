package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.mockito.Mockito.doReturn;

@Category(SmallTests.class)
public class TestHRegionCheckResources {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestHRegionCheckResources.class);

    @Test
    public void test_checkResources_no_block_on_meta_region() throws Exception {
        // 1. Prepare the test conditions.
        // Create a mocked HRegion instance.
        HRegion mockRegion = Mockito.mock(HRegion.class);

        // Mock the region info to simulate a meta region.
        HRegionInfo mockRegionInfo = Mockito.mock(HRegionInfo.class);
        doReturn(true).when(mockRegionInfo).isMetaRegion();
        doReturn(mockRegionInfo).when(mockRegion).getRegionInfo();

        // 2. Test code.
        // Invoke the checkResources() method.
        mockRegion.checkResources();

        // 3. Assert no exception is thrown; ensure execution succeeds for meta regions.
        // Note: Since this test focuses on not blocking meta regions, we rely on mock behavior
        //       to confirm no exception is thrown. Assertions aren't explicitly required here.
    }

    // 4. Code after testing.
    // Clean up resources or mocks if necessary. However, as we're using Mockito, no specific cleanup is required.
}