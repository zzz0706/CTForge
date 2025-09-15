package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;
//HDFS-8807
public class TestDataNodeDataDirConfig {

  @Test
  public void testDataDirConfigValueIsValid() {
    HdfsConfiguration conf = new HdfsConfiguration();
    String key = DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;

    // Fetch the raw comma-separated value
    String raw = conf.get(key);
    assertNotNull("Configuration " + key + " must be defined", raw);

    // Split into entries WITHOUT trimming them
    String[] entries = raw.split(",", -1);
    assertTrue("Expected at least one entry in " + key, entries.length > 0);

    for (String entry : entries) {
      // 1) Reject entries with any leading or trailing whitespace
      assertEquals(
        "Entry must not have leading or trailing whitespace: '" + entry + "'",
        entry, entry.trim()
      );

      // 2) Now check syntax: optional [TYPE], then immediately URI scheme
      assertTrue(
        "Invalid dataDir entry syntax: '" + entry + "'",
        entry.matches("^(\\[[A-Za-z]+\\])?\\S+://.+$")
      );
    }
  }
}
