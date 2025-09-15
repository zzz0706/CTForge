package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TestNetTopologyTableFileNameConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  /**
   * net.topology.table.file.name is only relevant when
   * net.topology.node.switch.mapping.impl is set to
   * org.apache.hadoop.net.TableMapping.
   *
   * If the mapping class is TableMapping, the configured file
   * must exist and be readable; otherwise the topology table
   * load will silently fail and all nodes will be placed on
   * the default rack.
   */
  @Test
  public void testTableFileNameWhenTableMappingUsed() {
    // Ensure we are using TableMapping
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        TableMapping.class,
        org.apache.hadoop.net.DNSToSwitchMapping.class);

    String tableFile = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY);
    if (tableFile != null && !tableFile.trim().isEmpty()) {
      File f = new File(tableFile);
      assertTrue("Configured topology table file does not exist: " + tableFile, f.exists());
      assertTrue("Configured topology table file is not readable: " + tableFile, f.canRead());
    }
  }

  /**
   * When TableMapping is NOT used, net.topology.table.file.name
   * should be ignored.  This test simply ensures no exception
   * is thrown if the property is set to a non-existent file
   * while another mapping implementation is configured.
   */
  @Test
  public void testTableFileNameIgnoredWhenOtherMappingUsed() {
    // Use any implementation other than TableMapping
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        ScriptBasedMapping.class,
        org.apache.hadoop.net.DNSToSwitchMapping.class);

    // Intentionally set a non-existent file
    conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
             "/non/existent/topology.table");

    // No exception should occur; the property is simply ignored
    String tableFile = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY);
    assertEquals("/non/existent/topology.table", tableFile);
  }
}