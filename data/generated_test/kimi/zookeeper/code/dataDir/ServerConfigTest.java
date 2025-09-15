package org.apache.zookeeper.server;

import org.apache.zookeeper.server.ServerConfig;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class ServerConfigTest {

    @Test
    public void dataDirUsedAsDefaultDataLogDir() {
        // 1. Configuration as Input – we use ServerConfig directly, no external Configuration object
        ServerConfig config = new ServerConfig();

        // 2. Dynamic Expected Value Calculation
        String expectedPath = "/tmp/zktest/data";

        // 3. Mock/Stub External Dependencies – none needed for this unit test

        // 4. Invoke the Method Under Test
        config.parse(new String[]{"2181", expectedPath});

        // 5. Assertions and Verification
        assertEquals(new File(expectedPath), config.getDataDir());
        assertEquals(new File(expectedPath), config.getDataLogDir());
    }
}