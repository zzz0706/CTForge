package alluxio.master.journal.ufs;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.Master;
import alluxio.master.journal.sink.JournalSink;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class UfsJournalCheckpointThreadTest {

  @Test
  public void customCheckpointPeriodIsRespected() throws Exception {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    PowerMockito.mockStatic(ServerConfiguration.class);
    long expectedCheckpointPeriodEntries = 500_000L;
    when(ServerConfiguration.getLong(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES))
        .thenReturn(expectedCheckpointPeriodEntries);

    // 3. Test code.
    Master mockMaster = mock(Master.class);
    UfsJournal mockJournal = mock(UfsJournal.class);
    when(mockJournal.getQuietPeriodMs()).thenReturn(1000L);
    Supplier<Set<JournalSink>> mockJournalSinks = () -> Collections.emptySet();

    UfsJournalCheckpointThread thread =
        new UfsJournalCheckpointThread(mockMaster, mockJournal, mockJournalSinks);

    java.lang.reflect.Field field =
        UfsJournalCheckpointThread.class.getDeclaredField("mCheckpointPeriodEntries");
    field.setAccessible(true);
    long actualCheckpointPeriodEntries = field.getLong(thread);

    // 4. Code after testing.
    assertEquals(expectedCheckpointPeriodEntries, actualCheckpointPeriodEntries);
  }
}