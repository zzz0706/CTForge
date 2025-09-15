package alluxio.conf;

import org.junit.Test;
import static org.junit.Assert.*;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.uri.URI;
//ALLUXIO-10257
public class MasterMountTableRootUfsConfigTest {

  @Test
  public void testRootUfsConfigValidity() {
    // Load default properties and build an InstancedConfiguration
    AlluxioProperties properties = new AlluxioProperties();
    InstancedConfiguration conf = new InstancedConfiguration(properties);

    // Retrieve the configured UFS root URI string (may be null, empty, or a local path)
    String ufsRoot = conf.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);

    // Allow the configuration to be empty, unset, or a local filesystem path
    if (ufsRoot == null || ufsRoot.isEmpty() || ufsRoot.startsWith("/")) {
      // Empty or absolute local path is considered valid
      return;
    }

    // Otherwise parse the URI string into an alluxio.uri.URI (throws on invalid format)
    URI uri = URI.Factory.create(ufsRoot);

    // Must be absolute (i.e. have a scheme)
    assertTrue("URI must be absolute (have a scheme)", uri.isAbsolute());

    // Extract and verify scheme
    String scheme = uri.getScheme();
    assertNotNull("Scheme should not be null", scheme);
    assertFalse("Scheme should not be empty", scheme.isEmpty());

    // Only allow known UFS schemes
    assertTrue(
      "Unsupported UFS scheme: " + scheme,
      scheme.equals("hdfs")   ||
      scheme.equals("s3a")    ||
      scheme.equals("viewfs") ||
      scheme.equals("file")   ||
      scheme.equals("oss")    ||
      scheme.equals("gcs")
    );
  }
}
