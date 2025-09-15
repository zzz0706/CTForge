package org.apache.hadoop.fs;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Time.class})
public class DFIntervalTest {

    @Test
    public void verifyDfCommandIsSkippedWhenCalledWithinInterval() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        long expectedInterval = conf.getLong("fs.df.interval", 60000L);

        // 2. Prepare the test conditions.
        File tempDir = File.createTempFile("df-test", "");
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        DF df = spy(new DF(tempDir, conf));

        // Mock the static Time.monotonicNow() to return controlled timestamps.
        PowerMockito.mockStatic(Time.class);
        PowerMockito.when(Time.monotonicNow())
                    .thenReturn(0L)               // first call
                    .thenReturn(expectedInterval - 1); // second call before interval elapsed

        // 3. Test code.
        df.getAvailable(); // first call triggers runDF
        df.getAvailable(); // second call should be throttled

        // 4. Code after testing.
        verify(df, times(2)).getAvailable();
    }
}