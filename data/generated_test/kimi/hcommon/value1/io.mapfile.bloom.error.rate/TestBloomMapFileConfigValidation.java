package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBloomMapFileConfigValidation {

  @Test
  public void testBloomErrorRateConfigValidity() {
    Configuration conf = new Configuration();
    // Do NOT set any value; rely on what is loaded from the config files
    float errorRate = conf.getFloat(
        CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_ERROR_RATE_KEY,
        0.005f);

    // Constraint: must be > 0 and < 1
    assertTrue("io.mapfile.bloom.error.rate must be > 0", errorRate > 0.0f);
    assertTrue("io.mapfile.bloom.error.rate must be < 1", errorRate < 1.0f);

    // Ensure the value is actually a valid float
    String raw = conf.getTrimmed(
        CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_ERROR_RATE_KEY);
    if (raw != null) {
      try {
        Float.parseFloat(raw);
      } catch (NumberFormatException e) {
        fail("io.mapfile.bloom.error.rate is not a valid float: " + raw);
      }
    }
  }
}