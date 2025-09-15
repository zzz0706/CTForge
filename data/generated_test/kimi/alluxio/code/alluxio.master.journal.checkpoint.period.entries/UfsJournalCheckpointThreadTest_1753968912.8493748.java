package alluxio.master.journal.ufs;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.Master;
import alluxio.master.journal.sink.JournalSink;
import alluxio.util.CommonUtils;

import org.junit.After;
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
@PrepareForTest({UfsJournalReader.class, UfsJournalCheckpointThread.class})
public class UfsJournalCheckpointThreadTest {

    @After
    public void after() {
        ServerConfiguration.reset();
    }

    @Test
    public void negativeOrZeroPeriodDisablesAutomaticCheckpoint() throws Exception {
        // 1. Use Alluxio 2.1.0 API to obtain configuration value
        ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 0);

        // 2. Prepare mocks
        Master mockMaster = mock(Master.class);
        UfsJournal mockJournal = mock(UfsJournal.class);
        Supplier<Set<JournalSink>> mockSinks = () -> Collections.emptySet();

        UfsJournalReader mockReader = mock(UfsJournalReader.class);
        when(mockReader.getNextSequenceNumber())
                .thenReturn(1L)
                .thenReturn(10L)
                .thenReturn(1000L);
        when(mockReader.advance())
                .thenReturn(UfsJournalReader.State.LOG);

        PowerMockito.whenNew(UfsJournalReader.class)
                .withAnyArguments()
                .thenReturn(mockReader);

        // 3. Test code: start the thread, feed it a few entries and stop it
        UfsJournalCheckpointThread thread = new UfsJournalCheckpointThread(mockMaster, mockJournal, mockSinks);
        Thread t = new Thread(thread);
        t.start();
        // Let it spin a couple of times
        CommonUtils.sleepMs(100);
        thread.stop();
        t.join();

        // 4. Verify no checkpoint was written
        verify(mockJournal, never()).write(any());
    }
}