package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class TestConnectionManagerIdleScanThreshold {

  @Test
  public void defaultIdleScanThresholdIsLoaded() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions.
    int expectedIdleScanThreshold = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);

    // Use a concrete subclass of Server to instantiate it
    Server server = new RPC.Builder(conf)
        .setProtocol(TestConnectionManagerIdleScanThreshold.class)
        .setInstance(new TestConnectionManagerIdleScanThreshold())
        .build();

    // 3. Test code.
    Field cmField = Server.class.getDeclaredField("connectionManager");
    cmField.setAccessible(true);
    Object connectionManager = cmField.get(server);

    Field thresholdField = connectionManager.getClass().getDeclaredField("idleScanThreshold");
    thresholdField.setAccessible(true);
    int actualIdleScanThreshold = (int) thresholdField.get(connectionManager);

    // 4. Code after testing.
    assertEquals(expectedIdleScanThreshold, actualIdleScanThreshold);
  }
}