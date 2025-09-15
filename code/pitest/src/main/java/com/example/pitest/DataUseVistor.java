package com.example.pitest;

import java.util.*;
import org.pitest.reloc.asm.MethodVisitor;  
import org.pitest.reloc.asm.Opcodes;
import org.pitest.mutationtest.engine.gregor.*;

class DataUseVisitor extends AbstractInsnMutator {

  DataUseVisitor(MethodMutatorFactory factory,
                 MethodInfo           methodInfo,
                 MutationContext      ctx,
                 MethodVisitor        delegate) {
    super(factory, methodInfo, ctx, delegate);
  }


  private static final Map<Integer, ZeroOperandMutation> MUTS;
  static {
   MUTS = new HashMap<>();
   MUTS.put(Opcodes.ILOAD, new InsnSubstitution(Opcodes.ISTORE, "Replace load with store"));
   MUTS.put(Opcodes.ISTORE, new InsnSubstitution(Opcodes.ILOAD, "Replace store with load"));
   MUTS.put(Opcodes.IINC, new InsnSubstitution(Opcodes.IINC, "Replace inc with dec"));
   MUTS.put(Opcodes.IINC, new InsnSubstitution(Opcodes.IINC, "Replace inc with dec"));
   
}

  @Override
  protected Map<Integer, ZeroOperandMutation> getMutations() {
    return MUTS;
  }
}
