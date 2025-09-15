package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class TestNetTopologyTableFileNameConfig {

  private Configuration conf;
  private File tempFile;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    if (tempFile != null && tempFile.exists()) {
      tempFile.delete();
    }
  }

  @Test
  public void testTableMappingFileExistsWhenImplIsTableMapping() throws IOException {
    // 1. Prepare test conditions: set impl to TableMapping and provide a valid table file
    conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
             "org.apache.hadoop.net.TableMapping");

    // Create a temporary file to satisfy the requirement
    tempFile = File.createTempFile("topology", ".txt");
    tempFile.deleteOnExit();
    conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
             tempFile.getAbsolutePath());

    // 2. Test code: ensure table file is specified and exists
    String tableFileName = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY);
    if (tableFileName == null || tableFileName.trim().isEmpty()) {
      fail("net.topology.table.file.name must be configured when "
           + "net.topology.node.switch.mapping.impl is set to org.apache.hadoop.net.TableMapping");
    }

    File file = new File(tableFileName);
    if (!file.exists() || !file.isFile()) {
      fail("Configured net.topology.table.file.name does not exist or is not a regular file: "
           + tableFileName);
    }
  }

  @Test
  public void testTableMappingFileNotRequiredWhenImplIsNotTableMapping() {
    // 1. Prepare test conditions: set impl to something other than TableMapping
    conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
             "org.apache.hadoop.net.ScriptBasedMapping");

    // 2. Test code: table file should be optional
    String tableFileName = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY);
    assertTrue("net.topology.table.file.name is optional when "
               + "net.topology.node.switch.mapping.impl is not org.apache.hadoop.net.TableMapping",
               tableFileName == null || tableFileName.trim().isEmpty());
  }
}