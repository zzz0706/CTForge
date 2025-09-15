package alluxio.network;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class TieredIdentityFactoryTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void fromStringRejectsUnknownTier() throws Exception {
    // 1. Obtain configuration via API
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Prepare test conditions: set the locality order
    conf.set(PropertyKey.LOCALITY_ORDER, "node,rack");

    // 3. Test code: invoke TieredIdentity.fromString with an unknown tier
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Unrecognized tier: dc");
    TieredIdentityFactory.fromString("rack=A,dc=us", conf);
  }
}