package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

// hadoop-6566 HDFS-4586
public class TestDataNodeMakeInstance {

  private static final String DFS_DN_DATA_DIR_KEY       = "dfs.datanode.data.dir";
  private static final String DFS_NN_RPC_ADDR_KEY       = "dfs.namenode.rpc-address";
  private static final String DFS_NN_SERVICE_RPC_ADDR_KEY = "dfs.namenode.servicerpc-address";

  private Configuration conf;
  private List<StorageLocation> dataDirs;
  private File tmpDir;

  @BeforeClass
  public static void enableAssertions() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    cl.setDefaultAssertionStatus(true);
  }

  @Before
  public void setUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.set(DFS_NN_RPC_ADDR_KEY,       "127.0.0.1:0");
    conf.set(DFS_NN_SERVICE_RPC_ADDR_KEY, "127.0.0.1:0");

    String[] dirs = conf.getTrimmedStrings(DFS_DN_DATA_DIR_KEY);
    assertEquals("There should be only one directory configured", 1, dirs.length);

    dataDirs = new ArrayList<>();
    for (String d : dirs) {
      dataDirs.add(StorageLocation.parse(d));
    }
    assertFalse("There should be at least one StorageLocation after parsing", dataDirs.isEmpty());
  }

  @Test
  public void testMakeInstanceSucceedsWhenDirWritable() throws IOException {
    DataNode dn = DataNode.makeInstance(dataDirs, conf, /*secure*/ null);
    assertNotNull("A DataNode instance should be returned for a writable directory", dn);
    dn.shutdown();
  }
}