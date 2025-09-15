package com.example.pitest;

import java.util.Set;

import org.pitest.reloc.asm.MethodVisitor;
import org.pitest.mutationtest.engine.gregor.*;

public enum DataModifierMutator implements MethodMutatorFactory {

  DATA_MODIFIER;

  @Override
  public MethodVisitor create(MutationContext ctx, MethodInfo info, MethodVisitor mv) {
    return new DataModifierVisitor(this, info, ctx, mv);
    Set<String> names = MethodNameLoader.loadMethodNamesFromFile();

    if (names.contains(info.getName())) {
      return new DataModifierVisitor(this, info, ctx, mv);
    }
    return mv;
  }

  @Override
  public String getGloballyUniqueId() {
    return getClass().getName();
  }

  @Override
  public String getName() {
    return name();
  }
}
