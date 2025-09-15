package alluxio.master.journal.ufs;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.Master;
import alluxio.master.journal.ufs.UfsJournalCheckpointThread;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.sink.JournalSink;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

public class UfsJournalCheckpointThreadTest {

    @Test
    public void testUfsJournalCheckpointThreadConstruction() {
        // 1. Use the Alluxio API to obtain the configuration value.
        long expectedCheckpointPeriodEntries = ServerConfiguration.getLong(
                PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);

        // 2. Mock objects to simulate the required dependencies.
        Master mockMaster = Mockito.mock(Master.class);
        UfsJournal mockJournal = Mockito.mock(UfsJournal.class);

        // Create a supplier for journal sinks.
        Supplier<Set<JournalSink>> mockJournalSinksSupplier = () -> Collections.emptySet();

        // 3. Create an instance of UfsJournalCheckpointThread using the constructor.
        UfsJournalCheckpointThread thread = new UfsJournalCheckpointThread(
                mockMaster,
                mockJournal,
                mockJournalSinksSupplier
        );

        // 4. Verify that the configuration value is correct using reflection since mCheckpointPeriodEntries is private.
        long actualCheckpointPeriodEntries;
        try {
            java.lang.reflect.Field checkpointField = UfsJournalCheckpointThread.class.getDeclaredField("mCheckpointPeriodEntries");
            checkpointField.setAccessible(true);
            actualCheckpointPeriodEntries = checkpointField.getLong(thread);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError("Unable to access mCheckpointPeriodEntries field", e);
        }

        // Verify the value.
        Assert.assertEquals(expectedCheckpointPeriodEntries, actualCheckpointPeriodEntries);
    }
}