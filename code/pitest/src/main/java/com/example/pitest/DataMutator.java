package com.example.pitest;

import java.util.Set;

import org.pitest.mutationtest.engine.gregor.*;
import org.pitest.reloc.asm.MethodVisitor;

public enum DataMutator implements MethodMutatorFactory {
    DATA;

    @Override
    public MethodVisitor create(MutationContext ctx, MethodInfo info, MethodVisitor mv) {
        return new DataVisitor(this, info, ctx, mv);
        
        Set<String> names = MethodNameLoader.loadMethodNamesFromFile();

        if (names.contains(info.getName())) {
            return new DataVisitor(this, info, ctx, mv);
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
