package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.*;

public class TestCheckpointEditsDirConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
    // Do NOT set any value in code – rely on the loaded configuration file
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testCheckpointEditsDirIsValidURIList() {
    // 1. Read the actual configuration value
    String editsDirValue = conf.get(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);

    // 2. Resolve default if necessary (same as dfs.namenode.checkpoint.dir)
    if (editsDirValue == null || editsDirValue.trim().isEmpty()) {
      editsDirValue = conf.get(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
    }

    // 3. Validate each comma-separated path is a valid URI
    if (editsDirValue != null && !editsDirValue.trim().isEmpty()) {
      String[] dirs = editsDirValue.split(",");
      for (String dir : dirs) {
        dir = dir.trim();
        try {
          URI.create(dir);
        } catch (IllegalArgumentException e) {
          fail("Invalid URI in dfs.namenode.checkpoint.edits.dir: " + dir);
        }
      }
    }

    // 4. Ensure the list can be parsed by production code
    List<URI> uris = FSImage.getCheckpointEditsDirs(conf, null);
    assertNotNull("Parsed URI list must not be null", uris);
    for (URI uri : uris) {
      assertNotNull("Each URI must be non-null", uri);
    }
  }
}