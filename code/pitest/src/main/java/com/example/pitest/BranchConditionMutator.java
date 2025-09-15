package com.example.pitest;

import java.util.Set;

import org.pitest.reloc.asm.MethodVisitor;  
import org.pitest.mutationtest.engine.gregor.*;  

public enum BranchConditionMutator implements MethodMutatorFactory {

  BRANCH_CONDITION;

  @Override
  public MethodVisitor create(MutationContext ctx,
                              MethodInfo info,
                              MethodVisitor mv) {
    return new BranchConditionVisitor(this, info, ctx, mv);
    Set<String> names = MethodNameLoader.loadMethodNamesFromFile();

    if (names.contains(info.getName())) {
      return new BranchConditionVisitor(this, info, ctx, mv);
    }
  }

  @Override public String getGloballyUniqueId() { return getClass().getName(); }
  @Override public String getName()             { return name(); }
}
