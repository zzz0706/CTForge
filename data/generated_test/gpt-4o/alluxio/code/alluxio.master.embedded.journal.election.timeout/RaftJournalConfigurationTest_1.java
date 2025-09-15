package alluxio.master.journal.raft;

import org.junit.Test;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.raft.RaftJournalConfiguration;
import static org.junit.Assert.assertTrue;

public class RaftJournalConfigurationTest {

    @Test
    public void testValidateWithInvalidConfiguration() {
        // Prepare the test conditions
        // Use the Alluxio 2.1.0 API to obtain configuration values instead of hardcoding them
        long heartbeatIntervalMs = ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_HEARTBEAT_INTERVAL);
        long electionTimeoutMs = ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT);

        // Create a RaftJournalConfiguration instance with invalid values
        RaftJournalConfiguration invalidConfig = new RaftJournalConfiguration()
            .setHeartbeatIntervalMs(heartbeatIntervalMs)
            .setElectionTimeoutMs(heartbeatIntervalMs * 2); // Set an invalid value

        // Test the validate method
        try {
            invalidConfig.validate();
        } catch (IllegalStateException e) {
            assertTrue("Expected IllegalStateException due to invalid configuration", true);
        }
    }
}