package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public class ClientConfigCoverageTest {

  @Test
  public void testMaxResponseLengthIsPropagatedToIpcStreams() throws Exception {
    // 1. Obtain configuration value through public API
    Configuration conf = new Configuration(false);
    long customLimit = 32 * 1024 * 1024L; // 32 MB
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH, (int) customLimit);

    // 2. Prepare test conditions: minimal objects to trigger setupIOstreams
    InetSocketAddress dummyAddr = new InetSocketAddress("localhost", 0);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");
    Client.ConnectionId connId = new Client.ConnectionId(
        dummyAddr,
        Text.class,
        ugi,
        0,
        RetryPolicies.RETRY_FOREVER,
        conf);

    // 3. Test code: trigger Connection creation and setupIOstreams via public call()
    Client client = new Client(null, conf);
    try {
      client.call(
          RPC.RpcKind.RPC_WRITABLE,
          new DummyWritable(),
          connId,
          0,
          new AtomicBoolean(false));
    } catch (IOException ignored) {
      // Expected due to mocked setup; we only care about configuration propagation
    }

    // 4. Verify maxResponseLength reached IpcStreams via reflection
    Field connectionsField = Client.class.getDeclaredField("connections");
    connectionsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.concurrent.ConcurrentHashMap<Client.ConnectionId, Object> connections =
        (java.util.concurrent.ConcurrentHashMap<Client.ConnectionId, Object>) connectionsField.get(client);
    Object connection = connections.get(connId);
    assertNotNull("Connection should have been created", connection);

    Field ipcStreamsField = connection.getClass().getDeclaredField("ipcStreams");
    ipcStreamsField.setAccessible(true);
    Object ipcStreams = ipcStreamsField.get(connection);
    assertNotNull("IpcStreams should have been initialized", ipcStreams);

    Field maxResponseLengthField = ipcStreams.getClass().getDeclaredField("maxResponseLength");
    maxResponseLengthField.setAccessible(true);
    long actualMaxResponseLength = (Long) maxResponseLengthField.get(ipcStreams);

    assertEquals(
        "IpcStreams should use configured maxResponseLength",
        customLimit,
        actualMaxResponseLength);
  }

  private static final class DummyWritable implements Writable {
    @Override
    public void write(java.io.DataOutput out) throws IOException {}
    @Override
    public void readFields(java.io.DataInput in) throws IOException {}
  }
}