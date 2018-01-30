package com.zhytnik.reactive.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

/**
 * @author Alexey Zhytnik
 * @since 27-Jan-18
 */
//TODO: transfer to LineReader
class MemoryAllocator implements Supplier<ByteBuffer> {

    private static final int PAGE_SIZE = 4096;
    private static final int MEMORY_SIZE = 4 * PAGE_SIZE;

    private ByteBuffer heap;
    private final ByteBuffer direct;

    MemoryAllocator() {
        direct = ByteBuffer
                .allocateDirect(MEMORY_SIZE)
                .order(ByteOrder.nativeOrder())
                .limit(0)
                .mark();
    }

    @Override
    public ByteBuffer get() {
        final ByteBuffer memory = isDirectMode() ? direct : trySwapToDirect();

        if (memory.capacity() - memory.limit() >= PAGE_SIZE) {
            return addBlankPage(memory, memory.limit());
        } else if (!isFull(memory)) {
            return compress(memory);
        } else {
            return swapToHeap(memory);
        }
    }

    private boolean isDirectMode() {
        return heap == null;
    }

    private ByteBuffer addBlankPage(ByteBuffer memory, int to) {
        return memory.position(to).limit(to + PAGE_SIZE);
    }

    private boolean isFull(ByteBuffer memory) {
        return memory.reset().position() < PAGE_SIZE;
    }

    private ByteBuffer compress(ByteBuffer memory) {
        final int start = memory.position();

        memory.compact();
        reset(memory);
        addBlankPage(memory, MEMORY_SIZE - start);
        return memory;
    }

    private void reset(ByteBuffer memory) {
        memory.position(0).mark();
    }

    private ByteBuffer trySwapToDirect() {
        if (heap.limit() - heap.reset().position() > (MEMORY_SIZE - PAGE_SIZE)) return heap;

        reset(direct);
        direct.put(heap);
        direct.position(0);
        direct.limit(heap.limit() - heap.position());

        heap = null;
        return direct;
    }

    private ByteBuffer swapToHeap(ByteBuffer memory) {
        final int payload = memory.capacity() - memory.position();

        final ByteBuffer target = ByteBuffer
                .allocate(2 * memory.capacity())
                .put(memory);

        reset(target);
        target.limit(payload + PAGE_SIZE);
        target.position(payload);

        heap = target;
        return target;
    }
}
