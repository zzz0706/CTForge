package alluxio.master.meta;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.CoreMasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.JournalSystem;
import alluxio.security.user.UserState;
import alluxio.util.executor.ExecutorServiceFactories;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultMetaMasterTest {

  private ExecutorService mExecutor;

  @Before
  public void before() {
    mExecutor = Executors.newSingleThreadExecutor();
    ServerConfiguration.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "1000");
  }

  @After
  public void after() {
    mExecutor.shutdownNow();
  }

  @Test
  public void nonHaStandbyMaster_doesNotScheduleMetaMasterSync() throws Exception {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    //    (Already done in @Before.)

    // 2. Prepare the test conditions.
    BlockMaster blockMaster = mock(BlockMaster.class);
    JournalSystem journalSystem = mock(JournalSystem.class);
    CoreMasterContext masterContext = mock(CoreMasterContext.class);
    when(masterContext.getUserState()).thenReturn(UserState.Factory.create(ServerConfiguration.global()));
    when(masterContext.getJournalSystem()).thenReturn(journalSystem);

    DefaultMetaMaster metaMaster = new DefaultMetaMaster(
        blockMaster,
        masterContext,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutor)
    );

    // 3. Test code.
    metaMaster.start(false); // standby

    // 4. Code after testing.
    // No-op: the test simply ensures no exception is thrown when starting a non-HA standby master.
  }
}