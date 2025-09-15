package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;
//HDFS-7875
public class TestFailedVolumesTolerated {

  @Test
  public void testFailedVolumesToleratedIsValid() {
    // Load configuration (reads hdfs-site.xml from classpath)
    HdfsConfiguration conf = new HdfsConfiguration();

    // Key and default for tolerated failures
    String key = DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY;
    int defaultVal = DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT;

    // Actual configured value (or default if not set)
    int tolerated = conf.getInt(key, defaultVal);

    // Determine how many data dirs are configured
    String[] dataDirs = conf.getTrimmedStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    int numDirs = dataDirs.length;

    // Assert the value is within [0, numDirs)
    assertTrue(
      String.format(
        "Configuration %s=%d is invalid; expected 0 <= value < %d (configured data dirs)",
        key, tolerated, numDirs
      ),
      tolerated >= 0 && tolerated < numDirs
    );
  }
}
