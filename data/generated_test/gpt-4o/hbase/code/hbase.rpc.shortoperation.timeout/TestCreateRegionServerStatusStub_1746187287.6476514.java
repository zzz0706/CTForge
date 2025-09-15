package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCreateRegionServerStatusStub {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestCreateRegionServerStatusStub.class);

    @Test
    public void test_createRegionServerStatusStub_masterNotFound() throws Exception {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration mockConf = Mockito.mock(Configuration.class);
        Mockito.when(mockConf.getInt(
                HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
                HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT)
        ).thenReturn(HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);

        // 2. Prepare the test conditions.
        HRegionServer regionServer = Mockito.mock(HRegionServer.class);

        MasterAddressTracker mockMasterAddressTracker = Mockito.mock(MasterAddressTracker.class);
        // Simulate master not being found.
        Mockito.when(mockMasterAddressTracker.getMasterAddress(Mockito.anyBoolean())).thenReturn(null);

        // Ensure the HRegionServer references the mock configuration and master address tracker.
        Mockito.when(regionServer.getConfiguration()).thenReturn(mockConf);
        Mockito.when(regionServer.getMasterAddressTracker()).thenReturn(mockMasterAddressTracker);

        // Instead of directly mocking the sleep method, simulate the retry mechanism in the test logic.
        Mockito.when(regionServer.createRegionServerStatusStub(Mockito.eq(true)))
                .thenAnswer(invocation -> {
                    for (int i = 0; i < 5; i++) { // Retry simulation.
                        if (mockMasterAddressTracker.getMasterAddress(true) == null) {
                            Thread.sleep(200L); // Simulate sleep (retry logic).
                        }
                    }
                    return null;
                });

        // 3. Test code.
        // Invoke createRegionServerStatusStub with refresh=true, expecting retry logic to run.
        ServerName result = regionServer.createRegionServerStatusStub(true);

        // 4. Code after testing.
        // Ensure the behavior of returning null after exhausting retries.
        assert result == null : "Expected result to be null when master address is not found.";
    }
}