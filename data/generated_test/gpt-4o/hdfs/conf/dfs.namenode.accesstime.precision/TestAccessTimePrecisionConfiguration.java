package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestAccessTimePrecisionConfiguration {

  @Test
  public void testAccessTimePrecisionConfigurationValidity() {
    // Prepare the configuration by loading it dynamically using the HDFS API
    Configuration conf = new Configuration();
    
    // Retrieve the configuration value from the system
    long accessTimePrecision = conf.getLong(
        "dfs.namenode.accesstime.precision", 3600000); // Default value is 3600000 (1 hour)

    // Test the constraints for the configuration:
    // 1. Valid range: 0 or > 0
    // 2. Value should be a valid long data type

    // Ensure the configuration value is valid
    // If the value is 0, access times for HDFS should be disabled
    assertTrue("Access time precision should be 0 or a positive number.",
        accessTimePrecision >= 0);

    // Code after testing: you could also assert specific expected behaviors
    if (accessTimePrecision == 0) {
      // Access times for HDFS are disabled
      assertFalse("Access times is disabled when precision is set to 0.",
          conf.getBoolean("dfs.namenode.accesstime.supported", true));
    } else {
      // Access time is supported if precision is greater than 0
      assertTrue("Access time is supported when precision is greater than 0.",
          conf.getBoolean("dfs.namenode.accesstime.supported", true));
    }
  }
}