package alluxio.master.journal.raft;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.raft.transport.CopycatGrpcTransport;
import alluxio.util.network.NetworkAddressUtils;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

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

  @Test
  public void electionTimeoutPropagatedToCopycatServer() throws Exception {
    // 1. Calculate expected value from configuration
    long expectedElectionTimeoutMs = ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT);

    // 2. Mock CopycatServer builder chain
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

    // Mock static builder method
    PowerMockito.mockStatic(CopycatServer.class);
    PowerMockito.when(CopycatServer.builder(any())).thenReturn(mockBuilder);

    // Mock static NetworkAddressUtils
    PowerMockito.mockStatic(NetworkAddressUtils.class);
    PowerMockito.when(NetworkAddressUtils.getConnectAddress(any(), any()))
        .thenReturn(new InetSocketAddress("localhost", 19200));

    // Mock Storage builder
    PowerMockito.mockStatic(Storage.class);
    Storage.Builder mockStorageBuilder = mock(Storage.Builder.class);
    when(mockStorageBuilder.withDirectory(any(File.class))).thenReturn(mockStorageBuilder);
    when(mockStorageBuilder.withStorageLevel(any())).thenReturn(mockStorageBuilder);
    when(mockStorageBuilder.withMinorCompactionInterval(any())).thenReturn(mockStorageBuilder);
    when(mockStorageBuilder.withMaxSegmentSize(anyInt())).thenReturn(mockStorageBuilder);
    when(mockStorageBuilder.build()).thenReturn(mock(Storage.class));
    PowerMockito.when(Storage.builder()).thenReturn(mockStorageBuilder);

    // 3. Create RaftJournalSystem via factory
    RaftJournalConfiguration journalConf = RaftJournalConfiguration.defaults(
        NetworkAddressUtils.ServiceType.MASTER_RAFT);
    journalConf.setPath(new File("/tmp/test-journal"));
    journalConf.setClusterAddresses(Collections.singletonList(
        new InetSocketAddress("localhost", 19200)));
    journalConf.setLocalAddress(new InetSocketAddress("localhost", 19200));

    RaftJournalSystem journalSystem = RaftJournalSystem.create(journalConf);

    // 4. Trigger server initialization via start() (which internally calls initServer)
    try {
      journalSystem.start();
    } catch (Exception e) {
      // expected due to mocks
    }

    // 5. Verify the timeout was passed correctly
    verify(mockBuilder).withElectionTimeout(Duration.ofMillis(expectedElectionTimeoutMs));
  }
}