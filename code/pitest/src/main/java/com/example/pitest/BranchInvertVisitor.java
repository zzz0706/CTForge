package com.example.pitest;

import java.util.Collections;
import java.util.Map;

import org.pitest.reloc.asm.Label;
import org.pitest.reloc.asm.Opcodes;
import org.pitest.mutationtest.engine.MutationIdentifier;
import org.pitest.mutationtest.engine.gregor.*;
import org.pitest.mutationtest.engine.gregor.AbstractInsnMutator;
import org.pitest.reloc.asm.MethodVisitor;

public class BranchInvertVisitor extends AbstractInsnMutator {

  private final MutationContext myContext;
  private final MethodMutatorFactory myFactory;

  public BranchInvertVisitor(MethodMutatorFactory factory,
                           MethodInfo methodInfo,
                           MutationContext ctx,
                           MethodVisitor delegate) {
    super(factory, methodInfo, ctx, delegate);
    this.myContext = ctx;
    this.myFactory = factory;
  }

  @Override
  protected Map<Integer, ZeroOperandMutation> getMutations() {
    return Collections.emptyMap();
  }

  @Override
  public void visitJumpInsn(int opcode, Label label) {
    int mutatedOp = opcode;
    String desc = null;

    switch (opcode) {
      case Opcodes.IFEQ:
        mutatedOp = Opcodes.IFNE;
        desc = "invert IFEQ → IFNE";
        break;
      case Opcodes.IFNE:
        mutatedOp = Opcodes.IFEQ;
        desc = "invert IFNE → IFEQ";
        break;
      case Opcodes.IF_ICMPEQ:
        mutatedOp = Opcodes.IF_ICMPNE;
        desc = "invert IF_ICMPEQ → IF_ICMPNE";
        break;
      case Opcodes.IF_ICMPNE:
        mutatedOp = Opcodes.IF_ICMPEQ;
        desc = "invert IF_ICMPNE → IF_ICMPEQ";
        break;
    }

    if (desc != null) {
      MutationIdentifier id = myContext.registerMutation(myFactory, desc);
      if (myContext.shouldMutate(id)) {
        super.visitJumpInsn(mutatedOp, label);
        return;
      }
    }
    super.visitJumpInsn(opcode, label);
  }
}
