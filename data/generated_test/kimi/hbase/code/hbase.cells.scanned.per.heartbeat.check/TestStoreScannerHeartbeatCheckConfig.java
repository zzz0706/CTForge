package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestStoreScannerHeartbeatCheckConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestStoreScannerHeartbeatCheckConfig.class);

    @Test
    public void testDefaultCellsPerHeartbeatCheckIsLoadedFromConfiguration() {
        // 1. Instantiate Configuration without explicit set
        Configuration conf = new Configuration();

        // 2. Compute expected value dynamically
        long expectedCellsPerHeartbeatCheck = conf.getLong(
                StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK,
                StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK);

        // 3. Create ScanInfo with the configuration
        ScanInfo scanInfo = new ScanInfo(
                conf,
                "testFamily".getBytes(),
                1,               // minVersions
                1,               // maxVersions
                Long.MAX_VALUE,  // ttl
                KeepDeletedCells.FALSE,
                64 * 1024,       // blockSize
                0,               // timeToPurgeDeletes
                CellComparator.getInstance(),
                false            // newVersionBehavior
        );

        // 4. Assert the value propagated correctly
        assertEquals(expectedCellsPerHeartbeatCheck, scanInfo.getCellsPerTimeoutCheck());
    }
}