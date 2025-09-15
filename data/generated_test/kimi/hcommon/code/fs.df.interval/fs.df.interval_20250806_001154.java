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
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
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
        verify(df, times(1)).runDF(); // only the first call should actually run the command
    }

    @Test
    public void testDfCommandExecutedAfterInterval() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setLong("fs.df.interval", 1000L); // set a short interval
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
                    .thenReturn(expectedInterval - 1) // second call before interval elapsed
                    .thenReturn(expectedInterval + 1); // third call after interval elapsed

        // 3. Test code.
        df.getAvailable(); // first call triggers runDF
        df.getAvailable(); // second call should be throttled
        df.getAvailable(); // third call should trigger runDF again

        // 4. Code after testing.
        verify(df, times(2)).runDF(); // first and third call should run the command
    }

    @Test
    public void testZeroIntervalAlwaysExecutesCommand() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setLong("fs.df.interval", 0L); // set interval to 0
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
                    .thenReturn(1L)               // second call immediately after
                    .thenReturn(2L);              // third call immediately after

        // 3. Test code.
        df.getAvailable(); // first call triggers runDF
        df.getAvailable(); // second call should trigger runDF again
        df.getAvailable(); // third call should trigger runDF again

        // 4. Code after testing.
        verify(df, times(3)).runDF(); // all calls should run the command
    }

    @Test
    public void testNegativeIntervalDefaultsToZero() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setLong("fs.df.interval", -1000L); // set negative interval
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
                    .thenReturn(1L)               // second call immediately after
                    .thenReturn(2L);              // third call immediately after

        // 3. Test code.
        df.getAvailable(); // first call triggers runDF
        df.getAvailable(); // second call should trigger runDF again
        df.getAvailable(); // third call should trigger runDF again

        // 4. Code after testing.
        verify(df, times(3)).runDF(); // all calls should run the command
    }
}