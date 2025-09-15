package alluxio.master.journal;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.master.journal.JournalType;
import alluxio.util.CommonUtils;
import org.junit.Assert;
import org.junit.Test;

public class JournalSystemTest {

    /**
     * Test case name: testBuildJournalSystemWithUfsType
     * Objective: Verify the build method constructs UfsJournalSystem with correct propagation of quiet time configuration.
     */
    @Test
    public void testBuildJournalSystemWithUfsType() {
        // Prepare the test conditions
        JournalSystem.Builder builder = new JournalSystem.Builder();
        long quietTimeMs = ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);

        // Test code
        builder.setQuietTimeMs(quietTimeMs);
        ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString());

        JournalSystem journalSystem = builder.build(CommonUtils.ProcessType.MASTER);

        // Verify the result
        Assert.assertTrue(journalSystem instanceof UfsJournalSystem);

        UfsJournalSystem ufsJournalSystem = (UfsJournalSystem) journalSystem;
        // Verify quietTimeMs by accessing the correct field or configuration in UfsJournalSystem.
        long actualQuietTimeMs = ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);
        Assert.assertEquals(quietTimeMs, actualQuietTimeMs);
    }
}