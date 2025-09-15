package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.FlushType;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRegionServerAccountingInitialization {

    // Ensure every HBase test class defines this rule
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestRegionServerAccountingInitialization.class);

    @Test
    public void testRegionServerAccountingInitialization() throws Exception {
        // 1. Load HBase configuration (no mini-cluster needed)
        Configuration conf = HBaseConfiguration.create();

        // 2. Prepare test conditions: set a custom memstore lower‐limit percentage
        conf.setFloat(MemorySizeUtil.MEMSTORE_SIZE_LOWER_LIMIT_KEY, 0.95f);

        // Obtain the configured lower‐mark percentage via the public API
        float memstoreLowerLimitPercent =
            MemorySizeUtil.getGlobalMemStoreHeapLowerMark(conf, true);

        // 3. Initialize the accounting component under test
        RegionServerAccounting rsa = new RegionServerAccounting(conf);

        // Compute expected low‐mark threshold in bytes
        long globalMemStoreLimit = rsa.getGlobalMemStoreLimit();
        long expectedLowThreshold = (long) (globalMemStoreLimit * memstoreLowerLimitPercent);

        // 4. Verify that the accounting initialized the low‐mark correctly
        long actualLowMark = rsa.getGlobalMemStoreLimitLowMark();
        assert actualLowMark == expectedLowThreshold
            : String.format("Expected low‐mark %d but got %d", expectedLowThreshold, actualLowMark);

        // 5. Verify flush behavior below the low‐water mark
        FlushType flushType = rsa.isAboveLowWaterMark();
        assert flushType == FlushType.NORMAL
            : "Expected FlushType.NORMAL when usage is below low‐mark";

        // 6. Cleanup: unset the test property so other tests aren’t affected
        conf.unset(MemorySizeUtil.MEMSTORE_SIZE_LOWER_LIMIT_KEY);
    }
}
