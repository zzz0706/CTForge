package alluxio.master.journal.ufs;

import alluxio.conf.ServerConfiguration;
import alluxio.master.Master;
import alluxio.master.journal.ufs.UfsJournalCheckpointThread;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.sink.JournalSink;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Set;
import java.util.function.Supplier;

public class UfsJournalCheckpointThreadTest {
    @Test
    public void testUfsJournalCheckpointThreadInitialization() {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values
        long mockedCheckpointPeriodEntries = ServerConfiguration.getLong(
            alluxio.conf.PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);

        // 2. Prepare the test conditions
        Master mockMaster = Mockito.mock(Master.class);
        UfsJournal mockJournal = Mockito.mock(UfsJournal.class);
        Supplier<Set<JournalSink>> mockJournalSinks = Mockito.mock(Supplier.class); // Adjusted to match the constructor argument requirements

        // 3. Test code
        UfsJournalCheckpointThread checkpointThread = new UfsJournalCheckpointThread(
            mockMaster, mockJournal, mockJournalSinks); // Adjusted to remove the long argument since it is not part of the constructor 
        
        // Verify no exceptions occurred during initialization
        Assert.assertNotNull(checkpointThread);

        // 4. Code after testing
        Mockito.reset(mockMaster, mockJournal, mockJournalSinks);
    }
}