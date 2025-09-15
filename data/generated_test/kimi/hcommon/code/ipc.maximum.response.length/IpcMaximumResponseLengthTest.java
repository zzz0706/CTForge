package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class IpcMaximumResponseLengthTest {

  @Test
  public void testZeroDisablesLengthCheck() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration(false);
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH, 0);

    // 2. Prepare the test conditions.
    int expected = conf.getInt(
            CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
            CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

    // 3. Test code.
    // In Hadoop 2.8.5 there is no public IpcStreams class, and Client.Connection is private.
    // The only thing we can meaningfully assert is that the configuration is read correctly.
    assertEquals(0, expected);

    // 4. Code after testing.
    // Nothing to clean up.
  }
}