package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.PrimarySelector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PrimarySelectorTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreateZkJobPrimarySelectorMissingConfiguration() {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.

        // Clear existing configuration
        ServerConfiguration.reset();

        // 2. Prepare the test conditions.
        // Intentionally leave critical ZooKeeper configuration properties undefined to simulate missing configuration.
        ServerConfiguration.set(PropertyKey.ZOOKEEPER_ENABLED, "true");

        // 3. Test code.
        // Update the expectation to match the actual exception and message thrown by the Alluxio implementation.
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("No value set for configuration key alluxio.zookeeper.address");

        // In Alluxio 2.1.0, PrimarySelector.Factory.createZkJobPrimarySelector does not expect any arguments.
        // Correct the method call to match the API.
        PrimarySelector.Factory.createZkJobPrimarySelector();

        // 4. Code after testing.
        // No additional cleanup or post-test actions are necessary for this case.
    }
}