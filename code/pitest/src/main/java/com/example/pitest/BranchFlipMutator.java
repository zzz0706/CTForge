package com.example.pitest;

import org.pitest.mutationtest.engine.gregor.MethodInfo;
import org.pitest.mutationtest.engine.gregor.MethodMutatorFactory;
import org.pitest.mutationtest.engine.gregor.MutationContext;
import org.pitest.reloc.asm.MethodVisitor;

public enum BranchFlipMutator implements MethodMutatorFactory {

  BRANCH_FLIP;

  @Override
  public MethodVisitor create(MutationContext ctx, MethodInfo info, MethodVisitor mv) {
    return new BranchFlipVisitor(this, info, ctx, mv);
  }

  @Override public String getGloballyUniqueId() { return getClass().getName(); }
  @Override public String getName()             { return name(); }
}
