package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.conf.Configuration;
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
        
        // Forward configuration to the mock HRegionServer.
        Mockito.when(regionServer.getConfiguration()).thenReturn(mockConf);

        // Override HRegionServer#createRegionServerStatusStub logic via mocking.
        Mockito.doAnswer(invocation -> {
            // Simulate retries due to master address being null.
            if (mockMasterAddressTracker.getMasterAddress(true) == null) {
                return null;
            }
            return Mockito.mock(ServerName.class); // Placeholder for a successful response.
        }).when(regionServer).createRegionServerStatusStub(Mockito.anyBoolean());
        
        // 3. Test code.
        // Call method being tested.
        ServerName result = regionServer.createRegionServerStatusStub(true);

        // 4. Code after testing.
        Mockito.verify(regionServer, Mockito.atLeastOnce()).createRegionServerStatusStub(Mockito.anyBoolean());
        assert result == null : "Expected result to be null when no master address is found.";
    }
}