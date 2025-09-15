package com.example.pitest;

import org.pitest.reloc.asm.Opcodes;
import org.pitest.reloc.asm.MethodVisitor;
import org.pitest.mutationtest.engine.MutationIdentifier;
import org.pitest.mutationtest.engine.gregor.*;

public enum ReturnConstantMutator implements MethodMutatorFactory {
  RETURN_CONSTANT_MUTATOR;

  @Override
  public MethodVisitor create(final MutationContext context,
                              final MethodInfo methodInfo,
                              final MethodVisitor delegate) {
    return new ReturnConstantVisitor(this, methodInfo, context, delegate);
  }

  @Override public String getGloballyUniqueId() { return getClass().getName(); }
  @Override public String getName()           { return name(); }

  private static class ReturnConstantVisitor extends MethodVisitor {
    private final MutationContext ctx;
    private final MethodMutatorFactory fac;

    ReturnConstantVisitor(final MethodMutatorFactory factory,
                          final MethodInfo methodInfo,
                          final MutationContext context,
                          final MethodVisitor delegate) {
      super(Opcodes.ASM7, delegate);
      this.ctx = context;
      this.fac = factory;
    }

    @Override
    public void visitInsn(final int opcode) {
      String desc = null;
      if (opcode == Opcodes.IRETURN) {
        desc = "Force int return to 0";
      } else if (opcode == Opcodes.ARETURN) {
        desc = "Force object return to null";
      }
      if (desc != null) {
        MutationIdentifier id = ctx.registerMutation(fac, desc);
        if (ctx.shouldMutate(id)) {
          if (opcode == Opcodes.IRETURN) {
            super.visitInsn(Opcodes.ICONST_0);
            super.visitInsn(Opcodes.IRETURN);
          } else {
            super.visitInsn(Opcodes.ACONST_NULL);
            super.visitInsn(Opcodes.ARETURN);
          }
          return;
        }
      }
      super.visitInsn(opcode);
    }
  }
}
