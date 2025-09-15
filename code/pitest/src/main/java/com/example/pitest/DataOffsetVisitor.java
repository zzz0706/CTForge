package com.example.pitest;

import java.util.Collections;
import java.util.Map;

import org.pitest.mutationtest.engine.MutationIdentifier;
import org.pitest.mutationtest.engine.gregor.AbstractInsnMutator;
import org.pitest.mutationtest.engine.gregor.MethodInfo;
import org.pitest.mutationtest.engine.gregor.MethodMutatorFactory;
import org.pitest.mutationtest.engine.gregor.MutationContext;
import org.pitest.mutationtest.engine.gregor.ZeroOperandMutation;
import org.pitest.reloc.asm.MethodVisitor;
import org.pitest.reloc.asm.Opcodes;

public class DataOffsetVisitor extends AbstractInsnMutator {

  private static final int OFFSET = 1;

  private final MutationContext myContext;
  private final MethodMutatorFactory myFactory;

  public DataOffsetVisitor(MethodMutatorFactory factory,
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
  public void visitIntInsn(int opcode, int operand) {
    if (opcode == Opcodes.BIPUSH || opcode == Opcodes.SIPUSH) {
      MutationIdentifier id =
          myContext.registerMutation(myFactory, "Add " + OFFSET + " to constant");
      if (myContext.shouldMutate(id)) {
        super.visitIntInsn(opcode, operand + OFFSET);
        return;
      }
    }
    super.visitIntInsn(opcode, operand);
  }

  @Override
  public void visitLdcInsn(Object cst) {
    if (cst instanceof Integer) {
      int value = ((Integer) cst).intValue();
      MutationIdentifier id =
          myContext.registerMutation(myFactory, "Add " + OFFSET + " to constant");
      if (myContext.shouldMutate(id)) {
        super.visitLdcInsn(value + OFFSET);
        return;
      }
    }
    super.visitLdcInsn(cst);
  }

  @Override
  public void visitInsn(int opcode) {
    if (opcode >= Opcodes.ICONST_M1 && opcode <= Opcodes.ICONST_5) {
      int value;
      switch (opcode) {
        case Opcodes.ICONST_M1: value = -1; break;
        case Opcodes.ICONST_0:  value = 0;  break;
        case Opcodes.ICONST_1:  value = 1;  break;
        case Opcodes.ICONST_2:  value = 2;  break;
        case Opcodes.ICONST_3:  value = 3;  break;
        case Opcodes.ICONST_4:  value = 4;  break;
        case Opcodes.ICONST_5:  value = 5;  break;
        default: value = 0; 
      }
      MutationIdentifier id =
          myContext.registerMutation(myFactory, "Add " + OFFSET + " to constant");
      if (myContext.shouldMutate(id)) {
        int mutated = value + OFFSET;
        switch (mutated) {
          case -1: super.visitInsn(Opcodes.ICONST_M1); break;
          case 0:  super.visitInsn(Opcodes.ICONST_0);  break;
          case 1:  super.visitInsn(Opcodes.ICONST_1);  break;
          case 2:  super.visitInsn(Opcodes.ICONST_2);  break;
          case 3:  super.visitInsn(Opcodes.ICONST_3);  break;
          case 4:  super.visitInsn(Opcodes.ICONST_4);  break;
          case 5:  super.visitInsn(Opcodes.ICONST_5);  break;
          default: super.visitLdcInsn(mutated);        break;
        }
        return;
      }
    }
    super.visitInsn(opcode);
  }
}
