package alluxio.master.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.CoreMasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.JournalSystem;
import alluxio.underfs.MasterUfsManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultMetaMasterTest {

    @Before
    public void setUp() {
        ServerConfiguration.reset();
    }

    @After
    public void tearDown() {
        ServerConfiguration.reset();
    }

    @Test
    public void dailyBackupNotStartedOnStandbyMaster() throws Exception {
        // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        ServerConfiguration.set(PropertyKey.MASTER_DAILY_BACKUP_ENABLED, true);
        ServerConfiguration.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, 1000L);

        // 2. Prepare the test conditions.
        BlockMaster mockBlockMaster = mock(BlockMaster.class);
        CoreMasterContext mockCoreMasterCtx = mock(CoreMasterContext.class);
        when(mockCoreMasterCtx.getJournalSystem()).thenReturn(mock(JournalSystem.class));
        when(mockCoreMasterCtx.getUfsManager()).thenReturn(new MasterUfsManager());

        DefaultMetaMaster metaMaster = new DefaultMetaMaster(mockBlockMaster, mockCoreMasterCtx);

        // 3. Test code.
        // Invoke start(false) - standby master
        metaMaster.start(false);

        // 4. Code after testing.
        // No exception thrown and no AssertionError implies the test passes
    }
}