package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.conf.Configuration;

import static org.junit.Assert.*;

@Category({RegionServerTests.class, SmallTests.class})
public class TestConfigurationValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestConfigurationValidation.class);

  /**
   * Test case for validating the configuration value of 
   * "hbase.cells.scanned.per.heartbeat.check".
   */
  @Test
  public void testCellsScannedPerHeartbeatCheckConfiguration() {
    Configuration configuration = new Configuration();
    
    // Retrieve the value of the configuration
    long cellsPerHeartbeatCheck = configuration.getLong(
        StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK,
        StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK
    );

    assertTrue(
        "Configuration 'hbase.cells.scanned.per.heartbeat.check' must be non-negative",
        cellsPerHeartbeatCheck >= 0
    );
  }
}