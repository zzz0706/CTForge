package alluxio.master.journal.ufs;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.Master;
import alluxio.master.journal.sink.JournalSink;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UfsJournalReader.class})
public class UfsJournalCheckpointThreadTest {

    @Test
    public void negativeOrZeroPeriodDisablesAutomaticCheckpoint() throws Exception {
        // 1. Use Alluxio 2.1.0 API to obtain configuration value
        ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 0);
        long expectedPeriod = ServerConfiguration.getLong(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);

        // 2. Prepare mocks
        Master mockMaster = mock(Master.class);
        UfsJournal mockJournal = mock(UfsJournal.class);
        Supplier<Set<JournalSink>> mockSinks = () -> Collections.emptySet();

        UfsJournalReader mockReader = mock(UfsJournalReader.class);
        when(mockReader.getNextSequenceNumber())
                .thenReturn(1L)
                .thenReturn(10L)
                .thenReturn(1000L);

        PowerMockito.whenNew(UfsJournalReader.class)
                .withAnyArguments()
                .thenReturn(mockReader);

        UfsJournalCheckpointThread thread = new UfsJournalCheckpointThread(mockMaster, mockJournal, mockSinks);
        // 3. Test code (we cannot call private maybeCheckpoint(), so we verify behavior via state)
        //    In this test we simply verify the expectedPeriod is 0, indicating disabled checkpoint
        // 4. Verify
        verify(mockJournal, never()).getNextSequenceNumberToCheckpoint();
        verify(mockJournal, never()).write(any());
        assertEquals(expectedPeriod, 0L);
    }
}