package com.example.pitest;

import java.util.Collections;
import java.util.Map;

import org.pitest.mutationtest.engine.MutationIdentifier;
import org.pitest.mutationtest.engine.gregor.AbstractInsnMutator;
import org.pitest.mutationtest.engine.gregor.MethodInfo;
import org.pitest.mutationtest.engine.gregor.MethodMutatorFactory;
import org.pitest.mutationtest.engine.gregor.MutationContext;
import org.pitest.mutationtest.engine.gregor.ZeroOperandMutation;
import org.pitest.reloc.asm.Label;
import org.pitest.reloc.asm.MethodVisitor;
import org.pitest.reloc.asm.Opcodes;

public class BranchFlipVisitor extends AbstractInsnMutator {

  private final MutationContext myContext;
  private final MethodMutatorFactory myFactory;

  public BranchFlipVisitor(MethodMutatorFactory factory,
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
    if (isConditional(opcode)) {
      MutationIdentifier id =
          myContext.registerMutation(myFactory, "Replace conditional with GOTO");
      if (myContext.shouldMutate(id)) {
        super.visitJumpInsn(Opcodes.GOTO, label);
        return;
      }
    }
    super.visitJumpInsn(opcode, label);
  }

  private boolean isConditional(int opcode) {
    switch (opcode) {
      case Opcodes.IFEQ:
      case Opcodes.IFNE:
      case Opcodes.IFLT:
      case Opcodes.IFGE:
      case Opcodes.IFGT:
      case Opcodes.IFLE:
      case Opcodes.IF_ICMPEQ:
      case Opcodes.IF_ICMPNE:
      case Opcodes.IF_ICMPLT:
      case Opcodes.IF_ICMPGE:
      case Opcodes.IF_ICMPGT:
      case Opcodes.IF_ICMPLE:
      case Opcodes.IF_ACMPEQ:
      case Opcodes.IF_ACMPNE:
      case Opcodes.IFNULL:
      case Opcodes.IFNONNULL:
        return true;
      default:
        return false;
    }
  }
}
