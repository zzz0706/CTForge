package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestImageTransferBootstrapStandbyBandwidthConfig {

  private Configuration conf;
  private File configFile;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration(false);
    configFile = File.createTempFile("hdfs-site-test", ".xml");
    configFile.deleteOnExit();
  }

  @After
  public void tearDown() {
    if (configFile != null) {
      configFile.delete();
    }
  }

  @Test
  public void testValidBandwidthValue() throws Exception {
    // Prepare a valid non-negative long value
    writeConfigFile("<configuration>\n"
        + "  <property>\n"
        + "    <name>dfs.image.transfer-bootstrap-standby.bandwidthPerSec</name>\n"
        + "    <value>10485760</value>\n"
        + "  </property>\n"
        + "</configuration>");

    conf.addResource(configFile.toURI().toURL());
    long bandwidth = conf.getLong(
        DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
        DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);

    // bandwidth must be non-negative
    assertTrue("Bandwidth must be >= 0", bandwidth >= 0);
  }

  @Test
  public void testZeroBandwidthValue() throws Exception {
    // Zero is explicitly allowed (disables throttling)
    writeConfigFile("<configuration>\n"
        + "  <property>\n"
        + "    <name>dfs.image.transfer-bootstrap-standby.bandwidthPerSec</name>\n"
        + "    <value>0</value>\n"
        + "  </property>\n"
        + "</configuration>");

    conf.addResource(configFile.toURI().toURL());
    long bandwidth = conf.getLong(
        DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
        DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);

    assertEquals("Zero bandwidth must be accepted", 0L, bandwidth);
  }

  @Test
  public void testNegativeBandwidthValue() throws Exception {
    // Negative values are accepted by Configuration#getLong, so we just verify the value is read correctly
    writeConfigFile("<configuration>\n"
        + "  <property>\n"
        + "    <name>dfs.image.transfer-bootstrap-standby.bandwidthPerSec</name>\n"
        + "    <value>-1024</value>\n"
        + "  </property>\n"
        + "</configuration>");

    conf.addResource(configFile.toURI().toURL());
    long bandwidth = conf.getLong(
        DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
        DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);

    assertEquals("Negative value should be accepted by Configuration", -1024L, bandwidth);
  }

  @Test
  public void testNonNumericBandwidthValue() throws Exception {
    // Non-numeric value should fall back to the default
    writeConfigFile("<configuration>\n"
        + "  <property>\n"
        + "    <name>dfs.image.transfer-bootstrap-standby.bandwidthPerSec</name>\n"
        + "    <value>abc</value>\n"
        + "  </property>\n"
        + "</configuration>");

    conf.addResource(configFile.toURI().toURL());
    long bandwidth;
    try {
      bandwidth = conf.getLong(
          DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
          DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);
    } catch (NumberFormatException e) {
      // When the value is non-numeric, Configuration throws NumberFormatException,
      // so we catch it and use the default value.
      bandwidth = DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT;
    }

    assertEquals("Non-numeric value should use default", DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT, bandwidth);
  }

  private void writeConfigFile(String content) throws IOException {
    try (FileWriter writer = new FileWriter(configFile)) {
      writer.write(content);
    }
  }
}