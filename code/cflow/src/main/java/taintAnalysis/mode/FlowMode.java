package taintAnalysis.mode;

/**
 * Enumeration of four configuration-flow analysis modes.
 */
public enum FlowMode {
    /** Explicit data-flow tainted by configuration. */
    EXPLICIT_DATA,
    /** Implicit data-flow through control dependence on configuration. */
    IMPLICIT_DATA,
    /** Explicit control-flow triggered by configuration. */
    EXPLICIT_CONTROL,
    /** Implicit control-flow (e.g. timing) influenced by configuration. */
    IMPLICIT_CONTROL
}

