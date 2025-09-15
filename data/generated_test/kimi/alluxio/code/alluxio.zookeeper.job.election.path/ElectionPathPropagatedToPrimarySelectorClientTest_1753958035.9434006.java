package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.PrimarySelector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PrimarySelector.class, PrimarySelectorClient.class})
public class ElectionPathPropagatedToPrimarySelectorClientTest {

  @Test
  public void electionPathPropagatedToPrimarySelectorClient() throws Exception {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    String expectedElectionPath = "/zk_test_job_election";

    // 2. Prepare the test conditions.
    ServerConfiguration.reset();
    ServerConfiguration.set(PropertyKey.ZOOKEEPER_ENABLED, true);
    ServerConfiguration.set(PropertyKey.ZOOKEEPER_ADDRESS, "zk://localhost:2181");
    ServerConfiguration.set(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH, expectedElectionPath);
    ServerConfiguration.set(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH, "/zk_test_job_leader");

    // Mock PrimarySelectorClient to capture the electionPath argument
    PrimarySelectorClient mockClient = PowerMockito.mock(PrimarySelectorClient.class);
    whenNew(PrimarySelectorClient.class)
        .withAnyArguments()
        .thenReturn(mockClient);

    // 3. Test code.
    PrimarySelector.Factory.createZkJobPrimarySelector();

    // 4. Code after testing.
    PowerMockito.verifyNew(PrimarySelectorClient.class, times(1))
        .withArguments("zk://localhost:2181", expectedElectionPath, "/zk_test_job_leader");
  }
}