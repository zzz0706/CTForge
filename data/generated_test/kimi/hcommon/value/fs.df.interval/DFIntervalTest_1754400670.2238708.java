package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Time.class})
public class DFIntervalTest {

    @Test
    public void testDfIntervalZeroTriggersImmediateReexecution() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        conf.setLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, 0L);

        // 2. Prepare the test conditions.
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        DF df = spy(new DF(tempDir, conf));

        PowerMockito.mockStatic(Time.class);
        PowerMockito.when(Time.monotonicNow())
                .thenReturn(0L)   // first call
                .thenReturn(1L);  // second call immediately after

        // 3. Test code.
        df.getAvailable();  // first invocation
        df.getAvailable();  // second invocation with interval=0 should re-execute

        // 4. Code after testing.
        verify(df, times(2)).getAvailable();
    }

    @Test
    public void testNegativeIntervalBehavesLikeZero() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        conf.setLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, -1000L);

        // 2. Prepare the test conditions.
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        DF df = spy(new DF(tempDir, conf));

        PowerMockito.mockStatic(Time.class);
        PowerMockito.when(Time.monotonicNow())
                .thenReturn(0L)
                .thenReturn(1L);

        // 3. Test code.
        df.getAvailable();
        df.getAvailable();

        // 4. Code after testing.
        verify(df, times(2)).getAvailable();
    }

    @Test
    public void testLargeIntervalPreventsReexecution() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        conf.setLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, Long.MAX_VALUE);

        // 2. Prepare the test conditions.
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        DF df = spy(new DF(tempDir, conf));

        PowerMockito.mockStatic(Time.class);
        PowerMockito.when(Time.monotonicNow())
                .thenReturn(0L)
                .thenReturn(1000L);

        // 3. Test code.
        df.getAvailable();
        df.getAvailable();

        // 4. Code after testing.
        verify(df, times(2)).getAvailable();
    }

    @Test
    public void testCustomIntervalBelowThreshold() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        conf.setLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, 5000L);

        // 2. Prepare the test conditions.
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        DF df = spy(new DF(tempDir, conf));

        PowerMockito.mockStatic(Time.class);
        PowerMockito.when(Time.monotonicNow())
                .thenReturn(0L)
                .thenReturn(4999L);

        // 3. Test code.
        df.getAvailable();
        df.getAvailable();

        // 4. Code after testing.
        verify(df, times(2)).getAvailable();
    }

    @Test
    public void testCustomIntervalAboveThreshold() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        conf.setLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, 5000L);

        // 2. Prepare the test conditions.
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        DF df = spy(new DF(tempDir, conf));

        PowerMockito.mockStatic(Time.class);
        PowerMockito.when(Time.monotonicNow())
                .thenReturn(0L)
                .thenReturn(5001L);

        // 3. Test code.
        df.getAvailable();
        df.getAvailable();

        // 4. Code after testing.
        verify(df, times(2)).getAvailable();
    }

    @Test
    public void testIntervalFromDefaultWhenNotSet() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false); // no explicit setting

        // 2. Prepare the test conditions.
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        DF df = spy(new DF(tempDir, conf));

        long expectedInterval = conf.getLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, DF.DF_INTERVAL_DEFAULT);

        PowerMockito.mockStatic(Time.class);
        PowerMockito.when(Time.monotonicNow())
                .thenReturn(0L)
                .thenReturn(expectedInterval - 1L);

        // 3. Test code.
        df.getAvailable();
        df.getAvailable();

        // 4. Code after testing.
        verify(df, times(2)).getAvailable();
    }
}