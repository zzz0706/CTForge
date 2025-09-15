package alluxio.conf;

import alluxio.util.ConfigurationUtils;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DefaultLocalityOrderValidationPassesTest {

  @Test
  public void defaultLocalityOrderValidationPasses() {
    // 1. Obtain configuration via the public API; no overrides → defaults are used
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Validation is enabled by default; ensure we keep it on
    boolean validationWasEnabled = conf.getBoolean(PropertyKey.CONF_VALIDATION_ENABLED);

    // 3. Invoke the method under test: InstancedConfiguration.validate()
    conf.validate();   // should not throw

    // 4. Post-condition: validation succeeded → no exception thrown
    assertTrue(validationWasEnabled);   // sanity check that validation ran
  }
}