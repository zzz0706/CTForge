package com.example.pitest;

import java.util.Collections;
import java.util.Map;

import org.objectweb.asm.Opcodes;
import org.pitest.reloc.asm.MethodVisitor;
import org.pitest.mutationtest.engine.MutationIdentifier;
import org.pitest.mutationtest.engine.gregor.AbstractInsnMutator;
import org.pitest.mutationtest.engine.gregor.MethodInfo;
import org.pitest.mutationtest.engine.gregor.MutationContext;
import org.pitest.mutationtest.engine.gregor.MethodMutatorFactory;
import org.pitest.mutationtest.engine.gregor.ZeroOperandMutation;


public class DataModifierVisitor extends AbstractInsnMutator {

  private static final int    C   = 5;      
  private static final String STR = "_X";   

  private final MutationContext myContext;
  private final MethodMutatorFactory myFactory;

  public DataModifierVisitor(MethodMutatorFactory factory,
                             MethodInfo           methodInfo,
                             MutationContext      ctx,
                             MethodVisitor        delegate) {
    super(factory, methodInfo, ctx, delegate);
    this.myContext = ctx;
    this.myFactory = factory;
  }

  @Override
  protected Map<Integer, ZeroOperandMutation> getMutations() {
    return Collections.emptyMap();
  }

  @Override
  public void visitVarInsn(int opcode, int var) {
    switch (opcode) {
      case Opcodes.ILOAD:
        super.visitVarInsn(opcode, var);
        MutationIdentifier intId = myContext.registerMutation(
            myFactory, "Add " + C + " to int var");
        if (myContext.shouldMutate(intId)) {
          super.visitLdcInsn(C);
          super.visitInsn(Opcodes.IADD);
        }
        return;

      case Opcodes.ALOAD:
        super.visitVarInsn(opcode, var);
        MutationIdentifier strId = myContext.registerMutation(
            myFactory, "Append \"" + STR + "\" to String var");
        if (myContext.shouldMutate(strId)) {
          super.visitLdcInsn(STR);
          super.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL,
              "java/lang/String",
              "concat",
              "(Ljava/lang/String;)Ljava/lang/String;",
              false);
        }
        return;

      default:
        super.visitVarInsn(opcode, var);
    }
  }
}
