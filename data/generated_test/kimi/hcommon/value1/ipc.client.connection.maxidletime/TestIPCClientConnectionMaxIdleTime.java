package org.apache.hadoop.ipc;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class TestIPCClientConnectionMaxIdleTime {

  @Test
  public void testMaxIdleTimeIsPositiveInt() {
    Configuration conf = new Configuration(false);
    // Do NOT set the value in code – read from test resources only
    int maxIdleTime = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);

    // 1. Must be a valid integer
    assertTrue("ipc.client.connection.maxidletime must be an integer",
               maxIdleTime > 0);

    // 2. Server side doubles it; ensure doubled value does not overflow
    long doubled = 2L * maxIdleTime;
    assertTrue("Doubled maxIdleTime must fit in a positive int",
               doubled > 0 && doubled <= Integer.MAX_VALUE);
  }
}