package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;
//HDFS-10269
public class TestFailedVolumesToleratedConfig {

  @Test
  public void testFailedVolumesToleratedRange() {
    HdfsConfiguration conf = new HdfsConfiguration();

    String key = DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY;
    int defaultVal = DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT; 

    int tolerated = conf.getInt(key, defaultVal);

    String[] dataDirs = conf.getTrimmedStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    int numDirs = dataDirs.length;

    assertTrue(
      String.format(
        "Configuration %s=%d is invalid; expected 0 <= value < %d",
        key, tolerated, numDirs
      ),
      tolerated >= 0 && tolerated < numDirs
    );
  }
}
