// LoopConditionMutator.java
package com.example.pitest;

import java.util.HashMap;
import java.util.Map;
import org.pitest.reloc.asm.Opcodes;
import org.pitest.mutationtest.engine.gregor.*;
import org.pitest.reloc.asm.MethodVisitor;

public enum LoopConditionMutator implements MethodMutatorFactory {
  LOOP_CONDITION_MUTATOR;

  @Override
  public MethodVisitor create(final MutationContext context,
                              final MethodInfo methodInfo,
                              final MethodVisitor delegate) {
    return new LoopConditionVisitor(this, methodInfo, context, delegate);
  }

  @Override public String getGloballyUniqueId() { return getClass().getName(); }
  @Override public String getName()           { return name(); }

  private static class LoopConditionVisitor extends AbstractInsnMutator {
    private static final Map<Integer, ZeroOperandMutation> MUTATIONS = new HashMap<>();
    static {
      MUTATIONS.put(Opcodes.IF_ICMPLT,
        new InsnSubstitution(Opcodes.IF_ICMPGE, "Invert < to >="));
      MUTATIONS.put(Opcodes.IF_ICMPGE,
        new InsnSubstitution(Opcodes.IF_ICMPLT, "Invert >= to <"));
    }

    LoopConditionVisitor(final MethodMutatorFactory factory,
                         final MethodInfo methodInfo,
                         final MutationContext ctx,
                         final MethodVisitor mv) {
      super(factory, methodInfo, ctx, mv);
    }

    @Override
    protected Map<Integer, ZeroOperandMutation> getMutations() {
      return MUTATIONS;
    }
  }
}
