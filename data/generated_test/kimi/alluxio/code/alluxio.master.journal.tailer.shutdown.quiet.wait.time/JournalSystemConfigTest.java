package alluxio.master.journal;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.util.CommonUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class JournalSystemConfigTest {

    @Test
    public void verifyQuietTimeMsFromCustomConfig() throws Exception {
        // 1. Use Alluxio 2.1.0 API to obtain the configuration value
        long expectedQuietTimeMs = 12000;

        // 2. Prepare test conditions
        PowerMockito.mockStatic(ServerConfiguration.class);
        PowerMockito.when(ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS))
                .thenReturn(expectedQuietTimeMs);
        PowerMockito.when(ServerConfiguration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class))
                .thenReturn(JournalType.UFS);

        // 3. Test code - build the journal system
        JournalSystem journalSystem = new JournalSystem.Builder()
                .setLocation(new URI("file:///tmp/journal"))
                .build(CommonUtils.ProcessType.MASTER);

        // 4. Assertions
        assertTrue("Should be instance of UfsJournalSystem", journalSystem instanceof UfsJournalSystem);

        // Access private field mQuietTimeMs via reflection
        Field quietTimeField = UfsJournalSystem.class.getDeclaredField("mQuietTimeMs");
        quietTimeField.setAccessible(true);
        long actualQuietTimeMs = (long) quietTimeField.get(journalSystem);

        assertEquals("mQuietTimeMs should equal expected value", expectedQuietTimeMs, actualQuietTimeMs);
    }
}