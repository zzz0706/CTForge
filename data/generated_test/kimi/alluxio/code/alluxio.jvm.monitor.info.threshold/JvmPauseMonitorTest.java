package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Thread.class, JvmPauseMonitor.class})
public class JvmPauseMonitorTest {

  private static final Logger LOG = Logger.getLogger(JvmPauseMonitor.class);

  @Mock
  private Appender mockAppender;

  @Captor
  private ArgumentCaptor<LoggingEvent> captor;

  private AlluxioConfiguration conf;

  @Before
  public void setup() {
    conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    Logger.getLogger(JvmPauseMonitor.class).addAppender(mockAppender);
    Logger.getLogger(JvmPauseMonitor.class).setLevel(Level.INFO);
  }

  @After
  public void tearDown() {
    Logger.getLogger(JvmPauseMonitor.class).removeAppender(mockAppender);
  }

  @Test
  public void verifyNoInfoLoggingWhenPauseBelowConfiguredThreshold() throws Exception {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    long infoThresholdMs = conf.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);
    long warnThresholdMs = conf.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
    long sleepIntervalMs = conf.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS);

    // 2. Prepare the test conditions.
    mockStatic(Thread.class);
    // Simulate extra delay that is smaller than the info threshold
    long simulatedExtra = infoThresholdMs - 100;
    doAnswer(invocation -> {
      Thread.sleep(sleepIntervalMs + simulatedExtra);
      return null;
    }).when(Thread.class);
    Thread.sleep(sleepIntervalMs);

    // 3. Test code.
    JvmPauseMonitor monitor = new JvmPauseMonitor(
        sleepIntervalMs,
        infoThresholdMs,
        warnThresholdMs
    );
    Thread monitorThread = new Thread(() -> monitor.start());
    monitorThread.start();
    // Allow one iteration
    Thread.sleep(sleepIntervalMs + simulatedExtra + 200);
    monitor.stop();
    monitorThread.join();

    // 4. Code after testing.
    verify(mockAppender, atLeast(0)).doAppend(captor.capture());
  }
}