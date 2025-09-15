package alluxio.master.journal.raft;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.raft.RaftJournalConfiguration;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RaftJournalConfigurationValidationTest {

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void electionTimeoutMustBeGreaterThanTwiceHeartbeatInterval() {
    // 1. Load configuration from the file (no programmatic override).
    // 2. Validate the dependency: heartbeat interval < election timeout / 2.
    long heartbeatMs = ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_HEARTBEAT_INTERVAL);
    long electionMs  = ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT);

    if (heartbeatMs >= electionMs / 2) {
      fail("Invalid configuration: heartbeat interval (" + heartbeatMs
           + "ms) must be less than half of the election timeout (" + electionMs + "ms)");
    }
  }

  @Test
  public void electionTimeoutMustBePositive() {
    long electionMs = ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT);
    if (electionMs <= 0) {
      fail("Invalid configuration: election timeout must be positive, found: " + electionMs + "ms");
    }
  }

  @Test
  public void raftJournalConfigurationValidatePassesWithValidDefaults() {
    // Build configuration using the defaults read from the configuration file.
    RaftJournalConfiguration conf = RaftJournalConfiguration.defaults(
        NetworkAddressUtils.ServiceType.MASTER_RAFT);

    try {
      conf.validate();
    } catch (IllegalStateException e) {
      fail("RaftJournalConfiguration validation failed: " + e.getMessage());
    }
  }
}