package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestIpcClientRpcTimeoutMsConfig {

  @Test
  public void testRpcTimeoutValueValid() {
    Configuration conf = new Configuration(false);
    conf.addResource("core-site.xml");

    int rpcTimeout = conf.getInt(
        CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);

    // Rule 2: must be non-negative int
    assertTrue("ipc.client.rpc-timeout.ms must be >= 0", rpcTimeout >= 0);

    // Rule 6: dependency with ipc.client.ping and ipc.ping.interval
    boolean pingEnabled = conf.getBoolean(
        CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
        CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT);
    if (pingEnabled && rpcTimeout > 0) {
      int pingInterval = conf.getInt(
          CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
          CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
      assertTrue("ipc.client.rpc-timeout.ms must be a multiple of ipc.ping.interval when ping is enabled",
          rpcTimeout % pingInterval == 0);
    }
  }
}