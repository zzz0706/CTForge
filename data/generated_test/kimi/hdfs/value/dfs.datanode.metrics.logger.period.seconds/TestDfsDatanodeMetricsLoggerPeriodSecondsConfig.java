package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class TestDfsDatanodeMetricsLoggerPeriodSecondsConfig {

  @Test
  public void testMetricsLoggerPeriodSecondsValid() {
    Configuration conf = new Configuration(false);
    // 1. Read the value from the configuration file (do NOT set it in code)
    int period = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
        DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT);

    // 2. Validate the constraint: must be >= 0 (0 disables logging)
    assertTrue("dfs.datanode.metrics.logger.period.seconds must be >= 0",
               period >= 0);
  }
}