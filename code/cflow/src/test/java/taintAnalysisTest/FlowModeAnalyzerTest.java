package taintAnalysisTest;

import org.junit.Test;
import org.junit.Assert;
import taintAnalysis.TaintAnalysisDriver;
import taintAnalysis.InterAnalysisTransformer;
import taintAnalysis.taintWrapper.ITaintWrapper;
import taintAnalysis.taintWrapper.TaintWrapper;
import taintAnalysis.sourceSinkManager.ISourceSinkManager;
import taintAnalysis.sourceSinkManager.SourceSinkManager;
import taintAnalysis.mode.FlowModeAnalyzer;
import utility.Config;

import java.util.List;

public class FlowModeAnalyzerTest {
    @Test
    public void testFlowModesGeneration() throws Exception {
        String[] cfg = Config.getCfg("test");
        List<String> srcPaths = Config.getSourcePaths(cfg);
        List<String> classPaths = Config.getClassPaths(cfg);
        ISourceSinkManager manager = new SourceSinkManager(Config.getInterface(cfg));
        ITaintWrapper wrapper = TaintWrapper.getDefault();
        TaintAnalysisDriver driver = new TaintAnalysisDriver(manager, wrapper);
        InterAnalysisTransformer transformer = driver.runInterTaintAnalysis(srcPaths, classPaths, false);
        FlowModeAnalyzer analyzer = transformer.getFlowAnalyzer();
        Assert.assertNotNull(analyzer);
    }
}

