package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCompactionConfigurationThrottleOverride {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestCompactionConfigurationThrottleOverride.class);

    @Test
    public void verifyExplicitThrottlePointOverride() {
        // 1. Instantiate Configuration
        Configuration conf = new Configuration();

        // 2. Set explicit value
        conf.setLong("hbase.regionserver.thread.compaction.throttle", 1073741824L);

        // 3. Mock StoreConfigInformation
        StoreConfigInformation mockStoreInfo = mock(StoreConfigInformation.class);
        when(mockStoreInfo.getMemStoreFlushSize()).thenReturn(134217728L);

        // 4. Create CompactionConfiguration
        CompactionConfiguration cc = new CompactionConfiguration(conf, mockStoreInfo);

        // 5. Get actual throttle point
        long actual = cc.getThrottlePoint();

        // 6. Assert expected value
        assertEquals(1073741824L, actual);
    }
}