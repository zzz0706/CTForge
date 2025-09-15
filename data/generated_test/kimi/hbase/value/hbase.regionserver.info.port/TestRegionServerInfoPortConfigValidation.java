package org.apache.hadoop.hbase.conf;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.SmallTests;

@Category(SmallTests.class)
public class TestRegionServerInfoPortConfigValidation {

  private static final String TEST_CONFIG_FILE = "test-hbase-site.xml";
  private static Configuration conf;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerInfoPortConfigValidation.class);

  @BeforeClass
  public static void setup() throws IOException {
    // 1. Prepare the test conditions.
    File file = new File(TEST_CONFIG_FILE);
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
      writer.write("<configuration>\n");
      writer.write("  <property>\n");
      writer.write("    <name>hbase.regionserver.info.port</name>\n");
      writer.write("    <value>16030</value>\n");
      writer.write("  </property>\n");
      writer.write("</configuration>\n");
    }

    // 2. Load configuration from file
    conf = HBaseConfiguration.create();
    conf.addResource(file.toURI().toURL());
  }

  @Test
  public void testRegionServerInfoPortValidation() {
    // 3. Test code.
    int port = conf.getInt(HConstants.REGIONSERVER_INFO_PORT, HConstants.DEFAULT_REGIONSERVER_INFOPORT);

    // Rule: port must be -1 (disabled) or a valid port number (0-65535)
    if (port != -1 && (port < 0 || port > 65535)) {
      fail("Invalid hbase.regionserver.info.port value: " + port +
           ". Must be -1 or between 0 and 65535.");
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    // 4. Code after testing.
    File file = new File(TEST_CONFIG_FILE);
    if (file.exists()) {
      file.delete();
    }
  }
}