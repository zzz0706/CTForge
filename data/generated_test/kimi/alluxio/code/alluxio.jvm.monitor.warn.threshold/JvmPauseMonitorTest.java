package alluxio.util;

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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Thread.class})
public class JvmPauseMonitorTest {

  private static final String WARN_THRESHOLD_KEY = "alluxio.jvm.monitor.warn.threshold";

  private InstancedConfiguration mConf;

  @Mock
  private Appender mMockAppender;

  @Captor
  private ArgumentCaptor<LoggingEvent> mLogCaptor;

  private Logger mLogger;

  @Before
  public void setUp() {
    mConf = new InstancedConfiguration(alluxio.conf.ServerConfiguration.global());
    mLogger = Logger.getLogger(JvmPauseMonitor.class);
    mLogger.addAppender(mMockAppender);
    mLogger.setLevel(Level.INFO);
  }

  @After
  public void tearDown() {
    mLogger.removeAppender(mMockAppender);
  }

  @Test
  public void noWarnBelowThreshold() throws Exception {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    long warnThresholdMs = mConf.getMs(PropertyKey.fromString(WARN_THRESHOLD_KEY));
    long sleepMs = 100L;
    long infoThresholdMs = 50L;

    // 2. Prepare the test conditions.
    mockStatic(Thread.class);
    PowerMockito.doNothing().when(Thread.class);
    Thread.sleep(sleepMs);

    // 3. Test code.
    JvmPauseMonitor monitor = new JvmPauseMonitor(
        sleepMs, infoThresholdMs, warnThresholdMs);
    monitor.start();
    Thread.sleep(300); // allow one iteration
    monitor.stop();

    // 4. Code after testing.
    verify(mMockAppender, atLeastOnce()).doAppend(mLogCaptor.capture());
    boolean foundWarn = mLogCaptor.getAllValues().stream()
        .anyMatch(e -> e.getLevel() == Level.WARN);
    assertTrue("No WARN log should appear", !foundWarn);
  }
}