package com.example.pitest;

import java.util.*;
import org.pitest.reloc.asm.MethodVisitor;  
import org.pitest.reloc.asm.Opcodes;
import org.pitest.mutationtest.engine.gregor.*;

class DataVisitor extends AbstractInsnMutator {

  DataVisitor(MethodMutatorFactory factory,
                 MethodInfo           methodInfo,
                 MutationContext      ctx,
                 MethodVisitor        delegate) {
    super(factory, methodInfo, ctx, delegate);
  }

  private static final Map<Integer, ZeroOperandMutation> MUTS;
  static {
    MUTS = new HashMap<>();
    MUTS.put(Opcodes.IADD, new InsnSubstitution(Opcodes.ISUB, "Replace + with -"));
    MUTS.put(Opcodes.ISUB, new InsnSubstitution(Opcodes.IADD, "Replace - with +"));
    MUTS.put(Opcodes.IMUL, new InsnSubstitution(Opcodes.IDIV, "Replace * with /"));
    MUTS.put(Opcodes.IDIV, new InsnSubstitution(Opcodes.IMUL, "Replace / with *"));
    MUTS.put(Opcodes.IREM, new InsnSubstitution(Opcodes.ISHL, "Replace % with <<"));
    MUTS.put(Opcodes.ISHL, new InsnSubstitution(Opcodes.IREM, "Replace << with %"));
    MUTS.put(Opcodes.IUSHR, new InsnSubstitution(Opcodes.ISHR, "Replace >>> with >>"));
    MUTS.put(Opcodes.ISHR, new InsnSubstitution(Opcodes.IUSHR, "Replace >> with >>>"));
  }

  @Override
  protected Map<Integer, ZeroOperandMutation> getMutations() {
    return MUTS;
  }
}
