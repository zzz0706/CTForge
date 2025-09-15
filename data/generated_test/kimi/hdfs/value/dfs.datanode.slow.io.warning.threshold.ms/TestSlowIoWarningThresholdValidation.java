package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class TestSlowIoWarningThresholdValidation {

  @Test
  public void testSlowIoWarningThresholdMsValid() {
    // 1. Obtain configuration value through HDFS 2.8.5 API
    Configuration conf = new Configuration();
    long threshold = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
        DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT);

    // 2. Prepare test conditions: none (value already loaded from config files)

    // 3. Test code: validate the threshold
    //    According to the code, the value is used as a positive millisecond
    //    threshold for logging slow operations.  It must be >= 0.
    assertTrue("dfs.datanode.slow.io.warning.threshold.ms must be >= 0",
               threshold >= 0);

    // 4. Code after testing: none
  }
}