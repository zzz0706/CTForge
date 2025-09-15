package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHbaseCellsScannedPerHeartbeatCheckConfig {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestHbaseCellsScannedPerHeartbeatCheckConfig.class);

  @Test
  public void testConfigValueValid() {
    Configuration conf = HBaseConfiguration.create();
    // Do NOT set the value – rely on whatever the test environment provides
    long value = conf.getLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK,
                              StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK);

    // Constraint: must be positive long
    assertTrue("hbase.cells.scanned.per.heartbeat.check must be > 0", value > 0);

    // If the user supplied zero or negative, the code silently falls back to the default
    // (ScanInfo#getCellsPerTimeoutCheck). Ensure that the resolved value is positive.
    long resolved = value > 0 ? value : StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK;
    assertTrue("Resolved cells-per-heartbeat-check must be > 0", resolved > 0);
  }
}