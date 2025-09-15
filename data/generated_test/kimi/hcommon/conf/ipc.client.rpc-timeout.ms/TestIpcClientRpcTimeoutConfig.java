package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestIpcClientRpcTimeoutConfig {

  @Test
  public void testRpcTimeoutConfigValidity() {
    // 1. Obtain configuration values via the HDFS 2.8.5 API
    Configuration conf = new Configuration(false);
    conf.addResource("hdfs-site.xml");   // let Hadoop load the actual file
    conf.addResource("core-site.xml");   // fallback

    // 2. Prepare the test conditions – nothing to set, we only *read*.

    // 3. Test code – validate the value against constraints and dependencies
    int rpcTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
                                 CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);

    // Rule: must be non-negative integer
    assertTrue("ipc.client.rpc-timeout.ms must be >= 0", rpcTimeout >= 0);

    // Rule: if ipc.client.ping is true and rpc-timeout is greater than
    //       ipc.ping.interval, the effective value is rounded up to a multiple
    //       of ipc.ping.interval.  We only check for the *input* validity here,
    //       not the rounding logic.
    boolean pingEnabled = conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
                                          CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT);
    int pingInterval = conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
                                   CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);

    if (pingEnabled && rpcTimeout > 0 && pingInterval > 0) {
      // Ensure the value is an integer multiple of pingInterval, otherwise warn
      if (rpcTimeout % pingInterval != 0) {
        // Not an error, but log for awareness
        System.err.println("WARN: ipc.client.rpc-timeout.ms (" + rpcTimeout +
                           ") is not a multiple of ipc.ping.interval (" +
                           pingInterval + "); effective timeout will be rounded up.");
      }
    }

    // 4. Nothing to tear down – purely a read-only validation test
  }
}