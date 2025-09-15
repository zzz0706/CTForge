package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public class ClientConfigTest {

  @Test
  public void testDefaultMaxResponseLengthIsUsed() throws Exception {
    // 1. Create fresh Configuration without overrides
    Configuration conf = new Configuration(false);

    // 2. Dynamically compute expected value from defaults
    long expectedMaxResponseLength = conf.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

    // 3. Prepare minimal objects to trigger Connection creation
    InetSocketAddress dummyAddr = new InetSocketAddress("localhost", 0);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");
    Client.ConnectionId connId = new Client.ConnectionId(
        dummyAddr,
        null,
        ugi,
        0,
        RetryPolicies.RETRY_FOREVER,
        conf);

    // 4. Trigger Connection creation via Client.call(...)
    Client client = new Client(null, conf);
    try {
      client.call(
          RPC.RpcKind.RPC_WRITABLE,
          null,
          connId,
          0,
          new AtomicBoolean(false));
    } catch (IOException ignored) {
      // Expected due to mocked setup; we only care about constructor capture
    }

    // 5. Verify the captured argument via reflection
    Field connectionsField = Client.class.getDeclaredField("connections");
    connectionsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.concurrent.ConcurrentHashMap<Client.ConnectionId, Object> connections =
        (java.util.concurrent.ConcurrentHashMap<Client.ConnectionId, Object>) connectionsField.get(client);
    Object connection = connections.get(connId);
    Field maxResponseLengthField = connection.getClass().getDeclaredField("maxResponseLength");
    maxResponseLengthField.setAccessible(true);
    long actualMaxResponseLength = (Long) maxResponseLengthField.get(connection);

    assertEquals(
        "Client should use default maxResponseLength",
        expectedMaxResponseLength,
        actualMaxResponseLength);
  }
}