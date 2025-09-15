// LogicalOperatorMutator.java
package com.example.pitest;

import java.util.HashMap;
import java.util.Map;
import org.pitest.reloc.asm.Opcodes;
import org.pitest.mutationtest.engine.gregor.*;
import org.pitest.reloc.asm.MethodVisitor;

public enum LogicalOperatorMutator implements MethodMutatorFactory {
  LOGICAL_OPERATOR_MUTATOR;

  @Override
  public MethodVisitor create(final MutationContext context,
                              final MethodInfo methodInfo,
                              final MethodVisitor delegate) {
    return new LogicalOperatorVisitor(this, methodInfo, context, delegate);
  }

  @Override public String getGloballyUniqueId() { return getClass().getName(); }
  @Override public String getName()           { return name(); }

  private static class LogicalOperatorVisitor extends AbstractInsnMutator {
    private static final Map<Integer, ZeroOperandMutation> MUTATIONS = new HashMap<>();
    static {
      MUTATIONS.put(Opcodes.IAND,
        new InsnSubstitution(Opcodes.IOR,  "Replace & with |"));
      MUTATIONS.put(Opcodes.IOR,
        new InsnSubstitution(Opcodes.IAND, "Replace | with &"));
      MUTATIONS.put(Opcodes.IXOR,
        new InsnSubstitution(Opcodes.IAND, "Replace ^ with &"));
    }
    LogicalOperatorVisitor(final MethodMutatorFactory factory,
                           final MethodInfo methodInfo,
                           final MutationContext ctx,
                           final MethodVisitor mv) {
      super(factory, methodInfo, ctx, mv);
    }
    @Override protected Map<Integer, ZeroOperandMutation> getMutations() {
      return MUTATIONS;
    }
  }
}
