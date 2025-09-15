package alluxio.conf;

import org.junit.Test;
import org.junit.Assert;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import java.net.InetSocketAddress;
import java.util.List;

public class ConfigurationValidationTest {
  
    /**
     * Test to validate the configuration `alluxio.job.master.embedded.journal.addresses` and its dependencies.
     */
    @Test
    public void testJobMasterEmbeddedJournalAddressesConfiguration() {
        // Step 1: Use the correct API to initialize the Alluxio configuration.
        AlluxioProperties properties = new AlluxioProperties();
        InstancedConfiguration conf = new InstancedConfiguration(properties);

        // Step 2: Check if the configuration for PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES is set.
        boolean isConfigSet = conf.isSet(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES);

        if (isConfigSet) {
            // 1. Read the configuration value.
            List<String> addresses = conf.getList(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, ",");

            // 2. Check constraints for the addresses.
            for (String address : addresses) {
                try {
                    // Split hostname and port based on format.
                    String[] parts = address.split(":");
                    Assert.assertTrue("Invalid configuration format. Expected 'hostname:port'.", parts.length == 2);

                    // Validate hostname.
                    String hostname = parts[0];
                    Assert.assertNotNull("Hostname cannot be null.", hostname);
                    Assert.assertFalse("Hostname cannot be empty.", hostname.isEmpty());
                    Assert.assertTrue("Invalid hostname format.", hostname.matches("^[a-zA-Z0-9.-]+$"));

                    // Validate port.
                    String portString = parts[1];
                    int port = Integer.parseInt(portString);
                    Assert.assertTrue("Port must be in the range 1-65535.", port >= 1 && port <= 65535);
                } catch (Exception e) {
                    Assert.fail("Invalid journal address: " + address + ". Error: " + e.getMessage());
                }
            }
        } else {
            // Step 3: If configuration is not set, verify fallback logic.
            List<InetSocketAddress> journalAddresses = ConfigurationUtils.getJobMasterEmbeddedJournalAddresses(conf);

            // Check that fallback logic produces valid outcomes.
            Assert.assertNotNull("Fallback journal addresses cannot be null.", journalAddresses);
            for (InetSocketAddress journalAddress : journalAddresses) {
                Assert.assertNotNull("Fallback journal address hostname cannot be null.", journalAddress.getHostName());
                Assert.assertFalse("Fallback journal address hostname cannot be empty.", journalAddress.getHostName().isEmpty());
                Assert.assertTrue("Fallback journal address hostname must match allowed format.", journalAddress.getHostName().matches("^[a-zA-Z0-9.-]+$"));
                Assert.assertTrue("Fallback journal address port must be in the range 1-65535.", journalAddress.getPort() >= 1 && journalAddress.getPort() <= 65535);
            }
        }
    }
}