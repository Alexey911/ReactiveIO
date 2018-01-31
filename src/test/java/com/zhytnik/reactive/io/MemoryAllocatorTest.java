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

        allocator.get().position(7 * 4096 + 4096 - 1).mark().put((byte) 7);

        final ByteBuffer compactedMemory = allocator.get();

        assertThat(compactedMemory.get(0)).isEqualTo((byte) 7);
        assertThat(compactedMemory.position()).isEqualTo(1);
        assertThat(compactedMemory.limit()).isEqualTo(1 + 4096);
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

        allocator.get().put((byte) 77);

        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();

        final ByteBuffer swapped = allocator.get();

        assertThat(swapped.isDirect()).isFalse();
        assertThat(swapped.position()).isEqualTo(8 * 4096);
        assertThat(swapped.limit()).isEqualTo(8 * 4096 + 4096);
        assertThat(swapped.reset().position()).isEqualTo(0);
        assertThat(swapped.get(4096)).isEqualTo((byte) 77);
        assertThat(swapped.capacity()).isGreaterThanOrEqualTo(9 * 4096);
    }

    @Test
    public void alwaysTriesToUseDirectMemory() {
        allocator.get();

        allocator
                .get()
                .position(4096 + 777).mark()
                .put((byte) 9);

        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();
        allocator.get();

        final ByteBuffer compacted = allocator.get();

        assertThat(compacted.isDirect()).isTrue();
        assertThat(compacted.position()).isEqualTo(8 * 4096 - 4096 - 777);
        assertThat(compacted.limit()).isEqualTo(8 * 4096 - 777);
        assertThat(compacted.reset().position()).isEqualTo(0);
        assertThat(compacted.get()).isEqualTo((byte) 9);

        final ByteBuffer heapMemory = allocator.get();

        assertThat(heapMemory.isDirect()).isFalse();
        heapMemory.position(heapMemory.limit() - 1).mark().put((byte) 7);

        final ByteBuffer swapped = allocator.get();

        assertThat(swapped.isDirect());
        assertThat(swapped.position()).isEqualTo(1);
        assertThat(swapped.limit()).isEqualTo(1 + 4096);
        assertThat(swapped.reset().position()).isEqualTo(0);
        assertThat(swapped.reset().get(0)).isEqualTo((byte) 7);
    }
}
