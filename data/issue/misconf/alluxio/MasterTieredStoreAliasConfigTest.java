package alluxio.conf;

import org.junit.Test;
import static org.junit.Assert.*;
//ALLUXIO-8997
public class MasterTieredStoreAliasConfigTest {

  @Test
  public void testTieredStoreAliasesMatchGlobal() {
    // Load default Alluxio properties and wrap in an InstancedConfiguration
    AlluxioProperties props = new AlluxioProperties();
    InstancedConfiguration conf = new InstancedConfiguration(props);

    // Dynamically look up how many tiers are configured on worker and master
    int workerLevels = conf.getInt(PropertyKey.fromString("alluxio.worker.tieredstore.levels"));
    int masterLevels = conf.getInt(PropertyKey.fromString("alluxio.master.tieredstore.global.levels"));


    // For each tier index, fetch both aliases via fromString(...) and compare
    for (int i = 0; i < workerLevels; i++) {
      String workerAliasKey = String.format("alluxio.worker.tieredstore.level%d.alias", i);
      String masterAliasKey = String.format("alluxio.master.tieredstore.global.level%d.alias", i);

      String workerAlias = conf.get(PropertyKey.fromString(workerAliasKey));
      String masterAlias = conf.get(PropertyKey.fromString(masterAliasKey));

      assertEquals(
        String.format("Alias mismatch at tier %d: worker=\"%s\" vs master=\"%s\"", 
                      i, workerAlias, masterAlias),
        masterAlias,
        workerAlias
      );
    }
  }
}
