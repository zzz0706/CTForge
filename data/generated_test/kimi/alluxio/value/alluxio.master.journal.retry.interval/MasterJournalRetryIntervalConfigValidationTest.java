package alluxio.master.journal;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MasterJournalRetryIntervalConfigValidationTest {

  private AlluxioConfiguration mOrigConf;

  @Before
  public void before() {
    mOrigConf = ServerConfiguration.global();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
    ServerConfiguration.merge(mOrigConf.toMap(), alluxio.conf.Source.RUNTIME);
  }

  @Test
  public void validateRetryIntervalPositive() {
    // 1. Use the alluxio2.1.0 API to obtain the configuration value.
    InstancedConfiguration conf = new InstancedConfiguration(ServerConfiguration.global());
    // 2. Prepare the test conditions – the configuration is already loaded.
    // 3. Test code – ensure the retry interval is positive.
    long intervalMs = conf.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL);
    assertTrue("alluxio.master.journal.flush.retry.interval must be positive (ms > 0)", intervalMs > 0);
    // 4. Code after testing – handled in @After.
  }
}