package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class TestDirectoryScanThreadsConfig {

  @Test
  public void testDirectoryScanThreadsConfig() {
    // 1. Use the HDFS 2.8.5 API to obtain the configuration value.
    Configuration conf = new Configuration();
    int threads = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

    // 2. Prepare the test conditions.
    //    No additional setup required; the configuration is read directly.

    // 3. Test code.
    //    The value must be a positive integer.
    assertTrue("dfs.datanode.directoryscan.threads must be a positive integer",
               threads > 0);

    // 4. Code after testing.
    //    No cleanup necessary.
  }
}