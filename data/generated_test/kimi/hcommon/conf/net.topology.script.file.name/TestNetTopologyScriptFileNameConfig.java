package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class TestNetTopologyScriptFileNameConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testNetTopologyScriptFileNameNotSet() {
    // net.topology.script.file.name is not set (null by default)
    // RawScriptBasedMapping should fall back to DEFAULT_RACK
    assertNull("When unset, the configuration should be null",
               conf.get("net.topology.script.file.name"));
  }

  @Test
  public void testNetTopologyScriptFileNameNonExistentFile() {
    String nonExistentPath = "/non/existent/script.sh";
    conf.set("net.topology.script.file.name", nonExistentPath);

    String value = conf.get("net.topology.script.file.name");
    assertNotNull("Configuration should return the set value", value);
    assertEquals("Should return the exact path we set", nonExistentPath, value);

    File f = new File(value);
    assertFalse("Configured path should not exist on the filesystem", f.exists());
  }

  @Test
  public void testNetTopologyScriptFileNameExistingRegularFile() throws IOException {
    File temp = File.createTempFile("topology", ".sh");
    temp.setExecutable(true);
    temp.deleteOnExit();

    conf.set("net.topology.script.file.name", temp.getAbsolutePath());

    String value = conf.get("net.topology.script.file.name");
    assertNotNull("Configuration should return the set value", value);

    File f = new File(value);
    assertTrue("Configured path should exist on the filesystem", f.exists());
    assertTrue("Configured path should be a regular file", f.isFile());
  }

  @Test
  public void testNetTopologyScriptFileNameDirectoryInsteadOfFile() throws IOException {
    File tempDir = new File(System.getProperty("java.io.tmpdir"), "topologyDir");
    tempDir.mkdir();
    tempDir.deleteOnExit();

    conf.set("net.topology.script.file.name", tempDir.getAbsolutePath());

    String value = conf.get("net.topology.script.file.name");
    assertNotNull("Configuration should return the set value", value);

    File f = new File(value);
    assertTrue("Configured path should exist on the filesystem", f.exists());
    assertTrue("Configured path should be a directory", f.isDirectory());
  }

  @Test
  public void testNetTopologyScriptFileNameEmptyString() {
    conf.set("net.topology.script.file.name", "");
    String value = conf.get("net.topology.script.file.name");
    assertNotNull("Empty string should still be returned", value);
    assertEquals("Empty string should be preserved", "", value);
  }
}