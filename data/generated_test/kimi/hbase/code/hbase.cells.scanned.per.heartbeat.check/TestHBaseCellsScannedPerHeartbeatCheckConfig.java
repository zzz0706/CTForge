package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHBaseCellsScannedPerHeartbeatCheckConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestHBaseCellsScannedPerHeartbeatCheckConfig.class);

    @Test
    public void testPositiveCustomCellsPerHeartbeatCheckIsLoadedFromConfiguration() {
        // 1. Configuration as input
        Configuration conf = new Configuration();
        // Set a custom positive value
        conf.setLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK, 5000L);

        // 2. Dynamic expected value calculation
        long expectedCellsPerTimeoutCheck = conf.getLong(
                StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK,
                StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK);

        // 3. Mock/stub external dependencies (none required for this test)

        // 4. Invoke the method under test
        ScanInfo scanInfo = new ScanInfo(
                conf,
                "testFamily".getBytes(),
                1,                         // minVersions
                1,                         // maxVersions
                0L,                        // ttl
                KeepDeletedCells.FALSE,
                64 * 1024,                 // blockSize
                0L,                        // timeToPurgeDeletes
                CellComparator.getInstance(),
                false                      // newVersionBehavior
        );

        // 5. Assertions and verification
        assertEquals(expectedCellsPerTimeoutCheck, scanInfo.getCellsPerTimeoutCheck());
    }
}