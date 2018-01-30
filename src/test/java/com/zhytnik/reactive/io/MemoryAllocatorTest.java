package com.zhytnik.reactive.io;

import com.zhytnik.reactive.io.LineReader.MemoryAllocator;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Alexey Zhytnik
 * @since 30-Jan-18
 */
public class MemoryAllocatorTest {

    MemoryAllocator allocator = new MemoryAllocator();

    @Test
    public void allocatesDirectMemory() {
        assertThat(allocator.get().isDirect()).isTrue();
    }

    @Test
    public void firstAllocatedMemoryHasMarkedStart() {
        assertThat(allocator.get().reset().position()).isEqualTo(0);
    }

    @Test
    public void alwaysAllocatesByPagesOf4KB() {
        final ByteBuffer memory = allocator.get();

        assertThat(memory.position() + 4096).isEqualTo(memory.limit());
    }

    @Test
    public void savesBytesFromMarkedPosition() {
        final ByteBuffer memory = allocator.get();

        memory.position(10).mark();

        memory.put(11, (byte) 1);
        memory.put(12, (byte) 2);
        memory.put(13, (byte) 3);

        memory.position(13).limit(13);

        final ByteBuffer nextPage = allocator.get();
        final int mark = nextPage.reset().position();

        assertThat(nextPage.get(mark + 1)).isEqualTo((byte) 1);
        assertThat(nextPage.get(mark + 2)).isEqualTo((byte) 2);
        assertThat(nextPage.get(mark + 3)).isEqualTo((byte) 3);
    }

    @Test
    public void compactsDirectMemory() {
        assertThat(allocator.get().position()).isEqualTo(0);
        assertThat(allocator.get().position()).isEqualTo(4096);
        assertThat(allocator.get().position()).isEqualTo(2 * 4096);
        assertThat(allocator.get().position()).isEqualTo(3 * 4096);
        assertThat(allocator.get().position()).isEqualTo(4 * 4096);
        assertThat(allocator.get().position()).isEqualTo(5 * 4096);
        assertThat(allocator.get().position()).isEqualTo(6 * 4096);

        allocator.get().position(8 * 4096 - 1).mark().put((byte) 7);

        final ByteBuffer compactedMemory = allocator.get();

        assertThat(compactedMemory.get(0)).isEqualTo((byte) 7);
        assertThat(compactedMemory.position()).isEqualTo(1);
        assertThat(compactedMemory.limit()).isEqualTo(4097);
    }

    @Test
    public void directMemoryIsLimitedBy32KB() {
        assertThat(allocator.get().isDirect());
        assertThat(allocator.get().isDirect());
        assertThat(allocator.get().isDirect());
        assertThat(allocator.get().isDirect());
        assertThat(allocator.get().isDirect());
        assertThat(allocator.get().isDirect());
        assertThat(allocator.get().isDirect());
        assertThat(allocator.get().isDirect());

        assertThat(allocator.get().isDirect()).isFalse();
    }

    @Test
    public void swapDataToHeapWhenDirectMemoryLimitIsReached() {
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();

        final ByteBuffer swapped = allocator.get();

        assertThat(swapped.isDirect()).isFalse();
        assertThat(swapped.limit()).isEqualTo(4096 * 9);
        assertThat(swapped.position()).isEqualTo(4096 * 8);
        assertThat(swapped.reset().position()).isEqualTo(0);
        assertThat(swapped.capacity()).isGreaterThanOrEqualTo(4096 * 9);
    }
}
