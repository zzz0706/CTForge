package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestIpcMaximumDataLengthConfig {

  @Test
  public void testIpcMaximumDataLengthValid() {
    Configuration conf = new Configuration(false);
    // Do NOT set any value – we read whatever the config file (or defaults) provides
    int maxDataLength = conf.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);

    // Constraint: must be a positive integer
    assertTrue("ipc.maximum.data.length must be a positive integer",
               maxDataLength > 0);

    // Constraint: must not exceed Integer.MAX_VALUE (sanity check)
    assertTrue("ipc.maximum.data.length must not exceed Integer.MAX_VALUE",
               maxDataLength <= Integer.MAX_VALUE);
  }
}