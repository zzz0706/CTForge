package alluxio.master.journal.raft;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.raft.transport.CopycatGrpcTransport;
import alluxio.util.network.NetworkAddressUtils;

import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CopycatServer.class, NetworkAddressUtils.class, Storage.class})
public class RaftJournalSystemElectionTimeoutTest {

  @Before
  public void setUp() {
    // Reset the global configuration so every test starts fresh
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    ServerConfiguration.reset();
  }

  @Test
  public void electionTimeoutPropagatedToCopycatServer() throws Exception {
    // 1. Use Alluxio 2.1.0 API to obtain the configured timeout value
    long expectedElectionTimeoutMs =
        ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT);

    // 2. Prepare test conditions: mock CopycatServer static builder chain
    CopycatServer.Builder mockBuilder = mock(CopycatServer.Builder.class);
    when(mockBuilder.withStorage(any())).thenReturn(mockBuilder);
    when(mockBuilder.withElectionTimeout(any(Duration.class))).thenReturn(mockBuilder);
    when(mockBuilder.withHeartbeatInterval(any(Duration.class))).thenReturn(mockBuilder);
    when(mockBuilder.withSnapshotAllowed(any())).thenReturn(mockBuilder);
    when(mockBuilder.withSerializer(any())).thenReturn(mockBuilder);
    when(mockBuilder.withTransport(any())).thenReturn(mockBuilder);
    when(mockBuilder.withStateMachine(any())).thenReturn(mockBuilder);
    when(mockBuilder.withAppenderBatchSize(anyInt())).thenReturn(mockBuilder);

    CopycatServer mockServer = mock(CopycatServer.class);
    when(mockBuilder.build()).thenReturn(mockServer);

    // Mock static CopycatServer.builder
    PowerMockito.mockStatic(CopycatServer.class);
    PowerMockito.when(CopycatServer.builder(any())).thenReturn(mockBuilder);

    // Mock static NetworkAddressUtils
    PowerMockito.mockStatic(NetworkAddressUtils.class);
    PowerMockito.when(NetworkAddressUtils.getConnectAddress(any(), any()))
        .thenReturn(new InetSocketAddress("localhost", 19200));

    // Mock Storage builder
    PowerMockito.mockStatic(Storage.class);
    Storage.Builder mockStorageBuilder = mock(Storage.Builder.class);
    when(mockStorageBuilder.withDirectory((File) any())).thenReturn(mockStorageBuilder);
    when(mockStorageBuilder.withStorageLevel(any())).thenReturn(mockStorageBuilder);
    when(mockStorageBuilder.withMinorCompactionInterval(any())).thenReturn(mockStorageBuilder);
    when(mockStorageBuilder.withMaxSegmentSize(anyInt())).thenReturn(mockStorageBuilder);
    when(mockStorageBuilder.build()).thenReturn(mock(Storage.class));
    PowerMockito.when(Storage.builder()).thenReturn(mockStorageBuilder);

    // 3. Create RaftJournalSystem via factory and trigger server initialization
    RaftJournalConfiguration journalConf = RaftJournalConfiguration.defaults(
        NetworkAddressUtils.ServiceType.MASTER_RAFT);
    journalConf.setPath(new File("/tmp/test-journal"));
    journalConf.setClusterAddresses(Collections.singletonList(
        new InetSocketAddress("localhost", 19200)));
    journalConf.setLocalAddress(new InetSocketAddress("localhost", 19200));

    RaftJournalSystem journalSystem = RaftJournalSystem.create(journalConf);

    try {
      journalSystem.start(); // internally calls initServer()
    } catch (Exception e) {
      // Expected due to mocks; swallow to continue verification
    }

    // 4. Verify the value supplied to withElectionTimeout equals the expected timeout
    verify(mockBuilder).withElectionTimeout(Duration.ofMillis(expectedElectionTimeoutMs));
  }
}