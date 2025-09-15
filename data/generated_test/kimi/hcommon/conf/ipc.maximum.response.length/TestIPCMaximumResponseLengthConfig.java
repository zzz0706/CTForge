package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestIPCMaximumResponseLengthConfig {

  @Test
  public void testIPCMaximumResponseLengthValid() {
    Configuration conf = new Configuration(false);
    // 1. Use the hadoop-common2.8.5 API to obtain configuration values
    int maxLen = conf.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

    // 2. Prepare the test conditions
    //    The value must be an int >= 0. 0 means disabled.
    // 3. Test code
    assertTrue("ipc.maximum.response.length must be an integer >= 0",
               maxLen >= 0);

    // 4. Code after testing
    // Nothing to clean up; we only validated the value.
  }
}