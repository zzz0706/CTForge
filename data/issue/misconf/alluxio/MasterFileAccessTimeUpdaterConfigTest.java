package alluxio.conf;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
//ALLUXIO-3331
public class MasterFileAccessTimeUpdaterConfigTest {

    @Test
    public void testShutdownTimeoutConfigValueValidity() {
        // Prepare the AlluxioProperties object required for InstancedConfiguration
        AlluxioProperties properties = new AlluxioProperties();

        // Create an instance of InstancedConfiguration with the AlluxioProperties
        InstancedConfiguration conf = new InstancedConfiguration(properties);

        // Retrieve and parse the shutdown timeout value in milliseconds using the
        // Configuration API
        long ms = conf.getMs(PropertyKey.MASTER_FILE_ACCESS_TIME_UPDATER_SHUTDOWN_TIMEOUT);
      
        // Validate: -1 indicates disabled timeout; otherwise it must be between 1000ms
        // and 10000ms inclusive
        assertTrue(
                String.format("Parsed %dms, expected -1 or between [1000, 10000]ms inclusive", ms),
                ms == -1L || (ms >= 1_000L && ms <= 10_000L));
    }
}
