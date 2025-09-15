package com.example.pitest;

import java.util.*;
import org.pitest.reloc.asm.MethodVisitor;  
import org.pitest.reloc.asm.Opcodes;
import org.pitest.mutationtest.engine.gregor.*;

class BranchConditionVisitor extends AbstractInsnMutator {

  BranchConditionVisitor(MethodMutatorFactory factory,
                 MethodInfo           methodInfo,
                 MutationContext      ctx,
                 MethodVisitor        delegate) {
    super(factory, methodInfo, ctx, delegate);
  }


  private static final Map<Integer, ZeroOperandMutation> MUTS;
  static {
    MUTS = new HashMap<>();
    MUTS.put(Opcodes.IFEQ, new InsnSubstitution(Opcodes.IFNE, "Replace == with !="));
    MUTS.put(Opcodes.IFNE, new InsnSubstitution(Opcodes.IFEQ, "Replace != with =="));
    MUTS.put(Opcodes.IFLT, new InsnSubstitution(Opcodes.IFGE, "Replace < with >="));
    MUTS.put(Opcodes.IFGE, new InsnSubstitution(Opcodes.IFLT, "Replace >= with <"));
    MUTS.put(Opcodes.IFGT, new InsnSubstitution(Opcodes.IFLE, "Replace > with <="));
    MUTS.put(Opcodes.IFLE, new InsnSubstitution(Opcodes.IFGT, "Replace <= with >"));
  }

  @Override
  protected Map<Integer, ZeroOperandMutation> getMutations() {
    return MUTS;
  }
}
