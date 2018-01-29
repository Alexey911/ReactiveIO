package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

/**
 * @author Alexey Zhytnik
 * @since 27-Jan-18
 */
class MemoryAllocator implements Supplier<ByteBuffer> {

    private static final int PAGE_SIZE = 2;

    private final ByteBuffer directMemory;

    MemoryAllocator() {
        ByteBuffer memory = ByteBuffer.allocateDirect(4 * PAGE_SIZE);
        memory.order(ByteOrder.nativeOrder());
        memory.limit(0);

        resetForRead(memory);

        this.directMemory = memory;
    }

    @Override
    public ByteBuffer get() {
        final ByteBuffer memory = directMemory;

        final int lastAccess = memory.limit();

        if (lastAccess + PAGE_SIZE <= memory.capacity()) {
            memory.position(lastAccess);
            memory.limit(lastAccess + PAGE_SIZE);
        } else {
            final int firstAccess = firstAccess(memory);

            if (memory.capacity() - firstAccess >= PAGE_SIZE) {

                memory.position(firstAccess).compact();

                resetForRead(memory);

                memory.position(lastAccess - firstAccess);
                memory.limit(memory.position() + PAGE_SIZE);
            } else {
                throw new UnsupportedOperationException("TODO: create use heap alternative");
            }
        }

        return memory;
    }

    private void resetForRead(ByteBuffer memory) {
        memory.position(0).mark();
    }

    private int firstAccess(ByteBuffer memory) {
        final int tmp = memory.position();
        final int pos = memory.reset().position();
        memory.position(tmp);
        return pos;
    }
}
