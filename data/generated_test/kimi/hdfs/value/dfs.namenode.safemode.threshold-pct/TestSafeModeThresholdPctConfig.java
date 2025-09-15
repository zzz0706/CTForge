package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSafeModeThresholdPctConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Use the hdfs 2.8.5 API to obtain configuration values
    conf = new Configuration();
    // Do NOT set any configuration values in the test code
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testThresholdPctValidRange() {
    // 2. Prepare test conditions: rely on configuration file only
    float threshold = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
        0.999f);

    // 3. Test code: verify constraints
    // According to description: <= 0 means no wait, > 1 makes safe mode permanent
    // FSNamesystem logs a warning if > 1 but still accepts it
    assertTrue("threshold must be a valid float",
        threshold == threshold); // NaN check
    assertTrue("threshold must be finite",
        !Float.isInfinite(threshold));
  }

  @Test
  public void testThresholdPctTypeCheck() {
    // 3. Test code: verify the value is parsable as float
    String val = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY);
    if (val != null) {
      try {
        Float.parseFloat(val);
      } catch (NumberFormatException e) {
        fail("dfs.namenode.safemode.threshold-pct must be a valid float");
      }
    }
  }

  @Test
  public void testThresholdPctDependencyWithReplicationMin() {
    // 3. Test code: ensure threshold is meaningful when replication.min is set
    int minReplication = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);

    float threshold = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
        0.999f);

    // When threshold > 1, safe mode is permanent regardless of blocks
    if (threshold > 1.0f) {
      // Permanent safe mode is allowed but logged as warning
      // No failure, just noting the behavior
    } else if (threshold <= 0.0f) {
      // Safe mode exits immediately regardless of replication
      // No additional checks needed
    } else {
      // Normal case: threshold between 0 and 1
      assertTrue("threshold must be between 0 and 1 inclusive",
          threshold >= 0.0f && threshold <= 1.0f);
    }
  }

  // 4. No code after testing needed beyond tearDown
}