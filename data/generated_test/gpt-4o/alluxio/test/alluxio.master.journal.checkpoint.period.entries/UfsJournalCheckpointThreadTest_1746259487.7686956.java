package alluxio.master.journal.ufs;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.ufs.UfsJournalCheckpointThread;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.Master;
import alluxio.master.journal.sink.JournalSink;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.Mockito.*;

public class UfsJournalCheckpointThreadTest {

    private UfsJournalCheckpointThread mCheckpointThread;
    private Master mMockMaster;
    private UfsJournal mMockJournal;
    private Supplier<Set<JournalSink>> mMockJournalSinks;

    @Before
    public void setUp() throws IOException, NoSuchFieldException, IllegalAccessException {
        // Prepare mock dependencies
        mMockMaster = mock(Master.class);
        mMockJournal = mock(UfsJournal.class);
        mMockJournalSinks = mock(Supplier.class);

        // Mock behavior for UfsJournal
        when(mMockJournal.getQuietPeriodMs()).thenReturn(1000L);
        when(mMockJournal.getNextSequenceNumberToCheckpoint()).thenReturn(0L);

        // Create a checkpoint thread instance with mocked dependencies
        mCheckpointThread = new UfsJournalCheckpointThread(
                mMockMaster, mMockJournal, mMockJournalSinks
        );
    }

    @Test
    public void testMaybeCheckpointSkipsCheckpoint() throws Exception {
        // Obtain the configuration value using the ServerConfiguration API
        long checkpointPeriodEntries = ServerConfiguration.getLong(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);

        // Mocking the current checkpoint sequence number
        long previousCheckpointSequenceNumber = 50L;
        long nextSequenceNumber = 100L;

        // Use reflection to set the private field mNextSequenceNumberToCheckpoint
        java.lang.reflect.Field nextSequenceField =
                UfsJournalCheckpointThread.class.getDeclaredField("mNextSequenceNumberToCheckpoint");
        nextSequenceField.setAccessible(true);
        nextSequenceField.set(mCheckpointThread, previousCheckpointSequenceNumber);

        // Calculate delta to determine if checkpointing should occur
        long delta = nextSequenceNumber - (long) nextSequenceField.get(mCheckpointThread);

        // Assert that delta is smaller than the checkpointPeriodEntries
        assert (delta < checkpointPeriodEntries); // Ensure this condition for test logic

        // Use reflection to invoke the private method maybeCheckpoint()
        java.lang.reflect.Method maybeCheckpointMethod =
                UfsJournalCheckpointThread.class.getDeclaredMethod("maybeCheckpoint");
        maybeCheckpointMethod.setAccessible(true);
        maybeCheckpointMethod.invoke(mCheckpointThread);

        // Verify that the checkpoint-related method is not invoked
        verify(mMockJournal, never()).getNextSequenceNumberToCheckpoint();
    }
}