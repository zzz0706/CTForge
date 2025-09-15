package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Time.class})
public class DFIntervalTest {

    @Test
    public void verifyDfCommandIsExecutedWhenCalledAfterInterval() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        long expectedInterval = conf.getLong("fs.df.interval", 60000L);

        // 2. Prepare the test conditions.
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        DF df = spy(new DF(tempDir, conf));

        PowerMockito.mockStatic(Time.class);
        PowerMockito.when(Time.monotonicNow())
                .thenReturn(0L)                     // first call
                .thenReturn(expectedInterval + 1);  // second call

        // 3. Test code.
        df.getAvailable();  // first invocation
        df.getAvailable();  // second invocation after interval elapsed

        // 4. Code after testing.
        verify(df, times(2)).getAvailable();
    }
}