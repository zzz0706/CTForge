package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestScanInfoCellsPerHeartbeatCheck {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestScanInfoCellsPerHeartbeatCheck.class);

  @Test
  public void testNegativeCustomCellsPerHeartbeatCheckFallsBackToDefault() {
    // 1. Prepare Configuration with a negative value
    Configuration conf = new Configuration();
    conf.setLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK, -1L);

    // 2. Compute expected default value dynamically
    long expectedCellsPerTimeoutCheck = StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK;

    // 3. Instantiate ScanInfo using the configuration
    ScanInfo scanInfo = new ScanInfo(
        conf,
        "family".getBytes(),
        1,   // minVersions
        1,   // maxVersions
        0L,  // ttl
        KeepDeletedCells.FALSE,
        64 * 1024, // blockSize
        0L,  // timeToPurgeDeletes
        CellComparator.getInstance(),
        false // newVersionBehavior
    );

    // 4. Assert fallback to default
    assertEquals(expectedCellsPerTimeoutCheck, scanInfo.getCellsPerTimeoutCheck());
  }

  @Test
  public void testZeroCustomCellsPerHeartbeatCheckFallsBackToDefault() {
    // 1. Prepare Configuration with a zero value
    Configuration conf = new Configuration();
    conf.setLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK, 0L);

    // 2. Compute expected default value dynamically
    long expectedCellsPerTimeoutCheck = StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK;

    // 3. Instantiate ScanInfo using the configuration
    ScanInfo scanInfo = new ScanInfo(
        conf,
        "family".getBytes(),
        1,   // minVersions
        1,   // maxVersions
        0L,  // ttl
        KeepDeletedCells.FALSE,
        64 * 1024, // blockSize
        0L,  // timeToPurgeDeletes
        CellComparator.getInstance(),
        false // newVersionBehavior
    );

    // 4. Assert fallback to default
    assertEquals(expectedCellsPerTimeoutCheck, scanInfo.getCellsPerTimeoutCheck());
  }

  @Test
  public void testPositiveCustomCellsPerHeartbeatCheckIsUsed() {
    // 1. Prepare Configuration with a positive value
    Configuration conf = new Configuration();
    long customValue = 5000L;
    conf.setLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK, customValue);

    // 2. Instantiate ScanInfo using the configuration
    ScanInfo scanInfo = new ScanInfo(
        conf,
        "family".getBytes(),
        1,   // minVersions
        1,   // maxVersions
        0L,  // ttl
        KeepDeletedCells.FALSE,
        64 * 1024, // blockSize
        0L,  // timeToPurgeDeletes
        CellComparator.getInstance(),
        false // newVersionBehavior
    );

    // 3. Assert custom value is used
    assertEquals(customValue, scanInfo.getCellsPerTimeoutCheck());
  }
}