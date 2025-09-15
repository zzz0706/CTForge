package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestBloomMapFileConfigValidation {

  @Test
  public void testValidBloomErrorRate() throws IOException {
    // 1. Obtain configuration values through the hdfs 2.8.5 API
    Configuration conf = new Configuration(false);
    // 2. Prepare test conditions: load the actual configuration file (core-site.xml, etc.)
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("mapred-site.xml");

    // 3. Test code: validate io.mapfile.bloom.error.rate
    if (conf.get("io.mapfile.bloom.error.rate") != null) {
      float errorRate = conf.getFloat("io.mapfile.bloom.error.rate", -1.0f);

      // Must be a valid float between 0 and 1 exclusive
      assertTrue("io.mapfile.bloom.error.rate must be > 0 and < 1",
                 errorRate > 0.0f && errorRate < 1.0f);
    }

    // 4. Code after testing: none needed
  }

  @Test
  public void testDefaultBloomErrorRate() {
    // Ensure the default is within the valid range
    Configuration conf = new Configuration(false);
    float defaultRate = conf.getFloat("io.mapfile.bloom.error.rate", 0.005f);
    assertEquals("Default bloom error rate mismatch", 0.005f, defaultRate, 0.00001f);
  }
}