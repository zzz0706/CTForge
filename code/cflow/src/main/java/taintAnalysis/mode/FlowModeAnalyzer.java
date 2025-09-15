package taintAnalysis.mode;

import soot.Body;
import soot.SootMethod;
import soot.Unit;
import soot.jimple.IfStmt;
import soot.jimple.InvokeExpr;
import soot.jimple.InstanceInvokeExpr;
import soot.toolkits.graph.BriefUnitGraph;
import soot.toolkits.graph.UnitGraph;
import taintAnalysis.Taint;

import java.util.*;

/**
 * Performs lightweight analysis to categorize statements affected by
 * configuration values into explicit/implicit data and control flows.
 * <p>
 * This class reuses the method summaries produced by the existing taint
 * analysis and inspects the control-flow graph of each method to derive
 * control-dependence based relationships.
 */
public class FlowModeAnalyzer {
    private final Map<SootMethod, Map<Taint, List<Set<Taint>>>> methodSummary;

    private final Set<Unit> explicitData;
    private final Set<Unit> implicitData;
    private final Set<Unit> explicitControl;
    private final Set<Unit> implicitControl;

    public FlowModeAnalyzer(Map<SootMethod, Map<Taint, List<Set<Taint>>>> methodSummary) {
        this.methodSummary = methodSummary;
        this.explicitData = new HashSet<>();
        this.implicitData = new HashSet<>();
        this.explicitControl = new HashSet<>();
        this.implicitControl = new HashSet<>();
    }

    /**
     * Runs the analysis over all methods stored in the method summary.
     */
    public void analyze() {
        for (SootMethod method : methodSummary.keySet()) {
            if (!method.hasActiveBody()) {
                continue;
            }
            Body body = method.retrieveActiveBody();
            UnitGraph cfg = new BriefUnitGraph(body);

            // collect all statements that appear in the explicit taint summary
            Map<Taint, List<Set<Taint>>> summaries = methodSummary.get(method);
            for (Map.Entry<Taint, List<Set<Taint>>> e : summaries.entrySet()) {
                collectStmt(e.getKey());
                for (Set<Taint> taints : e.getValue()) {
                    for (Taint t : taints) {
                        collectStmt(t);
                    }
                }
            }

            // traverse if statements whose condition is tainted
            for (Unit u : body.getUnits()) {
                if (!(u instanceof IfStmt)) continue;
                IfStmt ifStmt = (IfStmt) u;
                if (usesExplicitValue(ifStmt.getCondition())) {
                    // mark all reachable units from each branch up to merge
                    Unit thenStart = ifStmt.getTarget();
                    Unit elseStart = cfg.getSuccsOf(u).stream()
                            .filter(s -> s != thenStart).findFirst().orElse(null);
                    if (thenStart != null) markExplicitControl(cfg, thenStart, ifStmt);
                    if (elseStart != null) markExplicitControl(cfg, elseStart, ifStmt);

                    Unit join = findJoin(cfg, ifStmt);
                    if (join != null) {
                        implicitData.add(join);
                    }
                }
            }

            // identify implicit control flow from Thread.sleep in loops
            for (Unit u : body.getUnits()) {
                if (u.toString().contains("Thread.sleep")) {
                    InvokeExpr invoke = ((soot.jimple.Stmt) u).getInvokeExpr();
                    if (invoke != null && usesExplicitValue(invoke.getArg(0))) {
                        markLoop(cfg, u);
                    }
                }
            }
        }
    }

    private void collectStmt(Taint t) {
        if (t == null) return;
        Unit s = t.getStmt();
        if (s != null) {
            explicitData.add(s);
        }
    }

    /** Returns true if the given value originates from explicit data flow. */
    private boolean usesExplicitValue(soot.Value v) {
        for (Unit s : explicitData) {
            if (s instanceof soot.jimple.DefinitionStmt) {
                soot.Value left = ((soot.jimple.DefinitionStmt) s).getLeftOp();
                if (v.equivTo(left)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void markExplicitControl(UnitGraph cfg, Unit start, Unit until) {
        Queue<Unit> work = new LinkedList<>();
        Set<Unit> visited = new HashSet<>();
        work.add(start);
        while (!work.isEmpty()) {
            Unit u = work.remove();
            if (!visited.add(u)) continue;
            if (u == until) continue;
            explicitControl.add(u);
            for (Unit succ : cfg.getSuccsOf(u)) {
                work.add(succ);
            }
        }
    }

    private Unit findJoin(UnitGraph cfg, IfStmt ifStmt) {
        Unit thenStart = ifStmt.getTarget();
        Unit elseStart = cfg.getSuccsOf(ifStmt).stream()
                .filter(s -> s != thenStart).findFirst().orElse(null);
        if (thenStart == null || elseStart == null) return null;
        Set<Unit> reachThen = reachable(cfg, thenStart, ifStmt);
        Set<Unit> reachElse = reachable(cfg, elseStart, ifStmt);
        for (Unit u : reachThen) {
            if (reachElse.contains(u)) return u;
        }
        return null;
    }

    private Set<Unit> reachable(UnitGraph cfg, Unit start, Unit stop) {
        Set<Unit> visited = new HashSet<>();
        Deque<Unit> stack = new ArrayDeque<>();
        stack.push(start);
        while (!stack.isEmpty()) {
            Unit u = stack.pop();
            if (!visited.add(u)) continue;
            if (u == stop) continue;
            for (Unit succ : cfg.getSuccsOf(u)) {
                stack.push(succ);
            }
        }
        return visited;
    }

    private void markLoop(UnitGraph cfg, Unit inside) {
        // very coarse-grained: mark the whole method body as implicit control
        implicitControl.addAll(cfg); // add all units in cfg
    }

    /** Returns the statements for a specific flow mode. */
    public Set<Unit> getUnits(FlowMode mode) {
        switch (mode) {
            case EXPLICIT_DATA:
                return explicitData;
            case IMPLICIT_DATA:
                return implicitData;
            case EXPLICIT_CONTROL:
                return explicitControl;
            case IMPLICIT_CONTROL:
                return implicitControl;
            default:
                return Collections.emptySet();
        }
    }
}

