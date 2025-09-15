package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.*;
// hdfs-2820

public class TestSharedEditsConfigDrivenValidation {

  @Test
  public void testSharedEditsWithoutHANotAllowed() throws Exception {

    Configuration conf = new HdfsConfiguration();

    String sharedEditsDir = conf.get(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);

    Assume.assumeTrue("dfs.namenode.shared.edits.dir is not configured", 
                      sharedEditsDir != null && !sharedEditsDir.isEmpty());


    String nameDir = conf.get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    if (nameDir == null || nameDir.isEmpty()) {
      File tmp = Files.createTempDirectory("nn-test-").toFile();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, tmp.toURI().toString());
    }
    NameNode.format(conf);

    try {
      new NameNode(conf);
      fail("Expected startup to fail because shared edits are configured without HA being enabled");
    } catch (IOException e) {
      String msg = e.getMessage().toLowerCase();
      assertTrue("Exception message should contain a hint about 'shared edits dir': " + msg,
                 msg.contains("shared edits dir"));
    }
  }
}