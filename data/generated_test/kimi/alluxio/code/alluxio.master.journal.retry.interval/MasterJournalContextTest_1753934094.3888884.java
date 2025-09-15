package alluxio.master.journal;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class MasterJournalContextTest {

    @Test
    public void testCustomRetryIntervalPropagatedToRetryPolicy() throws Exception {
        // 1. Compute expected value using Alluxio API
        long expectedIntervalMs = ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL);

        // 2. Prepare test conditions
        PowerMockito.mockStatic(ServerConfiguration.class);
        PowerMockito.when(ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL))
                .thenReturn(3500L);

        // 3. Test code
        Field field = MasterJournalContext.class.getDeclaredField("FLUSH_RETRY_INTERVAL_MS");
        field.setAccessible(true);
        long actualIntervalMs = field.getLong(null);

        // 4. Code after testing
        assertEquals(3500L, actualIntervalMs);
    }
}