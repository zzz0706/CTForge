package org.apache.hadoop.ipc;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class TestIPCClientIdleThresholdConfig {

  @Test
  public void testValidIPCClientIdleThreshold() {
    Configuration conf = new Configuration(false);
    conf.addResource("core-site.xml");

    int idleThreshold = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);

    // idleScanThreshold must be a non-negative integer
    assertTrue("ipc.client.idlethreshold must be >= 0",
               idleThreshold >= 0);
  }
}