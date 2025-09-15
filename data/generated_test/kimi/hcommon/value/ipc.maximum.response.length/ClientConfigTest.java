package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
  public void testCustomMaxResponseLengthIsUsed() throws Exception {
    // 1. Create Configuration with a non-default override
    Configuration conf = new Configuration(false);
    int customLimit = 1024 * 1024; // 1 MB instead of 128 MB default
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH, customLimit);

    // 2. Compute expected value from configuration
    long expectedMaxResponseLength = conf.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

    // 3. Prepare minimal objects
    InetSocketAddress dummyAddr = new InetSocketAddress("localhost", 0);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");
    Client.ConnectionId connId = new Client.ConnectionId(
        dummyAddr,
        null,
        ugi,
        0,
        RetryPolicies.RETRY_FOREVER,
        conf);

    // 4. Trigger Connection creation
    Client client = new Client(null, conf);
    try {
      client.call(
          RPC.RpcKind.RPC_WRITABLE,
          null,
          connId,
          0,
          new AtomicBoolean(false));
    } catch (IOException ignored) {
      // Expected
    }

    // 5. Verify the captured argument
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
        "Client should use custom maxResponseLength",
        expectedMaxResponseLength,
        actualMaxResponseLength);
    assertTrue("Custom limit should differ from default",
        actualMaxResponseLength != CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);
  }

  @Test
  public void testZeroMaxResponseLengthDisablesLimit() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH, 0);

    InetSocketAddress dummyAddr = new InetSocketAddress("localhost", 0);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");
    Client.ConnectionId connId = new Client.ConnectionId(
        dummyAddr,
        null,
        ugi,
        0,
        RetryPolicies.RETRY_FOREVER,
        conf);

    Client client = new Client(null, conf);
    try {
      client.call(
          RPC.RpcKind.RPC_WRITABLE,
          null,
          connId,
          0,
          new AtomicBoolean(false));
    } catch (IOException ignored) {
      // Expected
    }

    Field connectionsField = Client.class.getDeclaredField("connections");
    connectionsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.concurrent.ConcurrentHashMap<Client.ConnectionId, Object> connections =
        (java.util.concurrent.ConcurrentHashMap<Client.ConnectionId, Object>) connectionsField.get(client);
    Object connection = connections.get(connId);
    Field maxResponseLengthField = connection.getClass().getDeclaredField("maxResponseLength");
    maxResponseLengthField.setAccessible(true);
    long actualMaxResponseLength = (Long) maxResponseLengthField.get(connection);

    assertEquals("Zero should disable limit", 0, actualMaxResponseLength);
  }

  @Test
  public void testNegativeMaxResponseLengthIsRejected() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH, -1);

    InetSocketAddress dummyAddr = new InetSocketAddress("localhost", 0);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");
    Client.ConnectionId connId = new Client.ConnectionId(
        dummyAddr,
        null,
        ugi,
        0,
        RetryPolicies.RETRY_FOREVER,
        conf);

    Client client = new Client(null, conf);
    try {
      client.call(
          RPC.RpcKind.RPC_WRITABLE,
          null,
          connId,
          0,
          new AtomicBoolean(false));
    } catch (IOException ignored) {
      // Expected
    }

    Field connectionsField = Client.class.getDeclaredField("connections");
    connectionsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.concurrent.ConcurrentHashMap<Client.ConnectionId, Object> connections =
        (java.util.concurrent.ConcurrentHashMap<Client.ConnectionId, Object>) connectionsField.get(client);
    Object connection = connections.get(connId);
    Field maxResponseLengthField = connection.getClass().getDeclaredField("maxResponseLength");
    maxResponseLengthField.setAccessible(true);
    long actualMaxResponseLength = (Long) maxResponseLengthField.get(connection);

    assertEquals("Negative value should be treated as default",
        CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT,
        actualMaxResponseLength);
  }
}