package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class InstancedConfigurationTest {

    private InstancedConfiguration mConf;

    @Before
    public void setUp() {
        // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        AlluxioProperties props = new AlluxioProperties();
        props.set(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, "1sec");
        props.set(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "10000");
        mConf = new InstancedConfiguration(props);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void validationWarnsWhenWaitTimeShorterThanRetrySleep() {
        // 3. Test code.
        mConf.validate();

        // 4. Code after testing.
        long waitTime = mConf.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
        long retryInterval = mConf.getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

        assertTrue("Wait time should be 1000ms", waitTime == 1000L);
        assertTrue("Retry interval should be 10000ms", retryInterval == 10000L);
    }
}