package com.zhytnik.reactive.io;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Flow.Subscription;

import static java.lang.Long.MAX_VALUE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Alexey Zhytnik
 * @since 25.01.2018
 */
public class LineReaderTest {

    @ClassRule
    public static TemporaryFolder files = new TemporaryFolder();

    File file;
    LineReader reader;
    ReadAssertionSubscriber subscriber;

    @Before
    public void setUp() throws Exception {
        file = files.newFile();
        reader = new LineReader(file.toPath());
        subscriber = new ReadAssertionSubscriber();
    }

    @Test
    public void readsMultilineText() {
        read(
                '0', '1', '2', '3', '\n',
                '4', '5', '\r', '\n',
                '6', '7', '\r',
                '8'
        );

        assertThat(subscriber.items).containsExactly("0123", "45", "67", "8");
    }

    @Test
    public void readsLinesWithDifferentEnds() {
        read(
                '0', '\r',
                '2', '\n',
                '6', '\r',
                '7', '\r', '\n',
                '8'
        );

        assertThat(subscriber.items).containsExactly("0", "2", "6", "7", "8");
    }

    @Test
    public void readsEmptyFile() {
        read();

        assertThat(subscriber.items).isEmpty();
    }

    @Test
    public void readsSingleCharLine() {
        read('8');

        assertThat(subscriber.items).containsExactly("8");
    }

    @Test
    public void readsSingleLine() {
        read('4', '\r', '\n');

        assertThat(subscriber.items).containsExactly("4");
    }

    @Test
    public void readsRepeatableEmptyLines() {
        read(
                '0', '\n',
                '\r', '\n',
                '6', '\r',
                '8'
        );

        assertThat(subscriber.items).containsExactly("0", "", "6", "8");
    }

    @Test
    public void readsTextWithEmptyLines() {
        read('\r', '\r');

        assertThat(subscriber.items).containsExactly("", "");
    }

    @After
    public void validate() {
        subscriber.validate();
    }

    void read(char... chars) {
        writeToFile(chars);
        subscriber.request = MAX_VALUE;
        reader.subscribe(subscriber);
    }

    void writeToFile(char... chars) {
        final byte[] bytes = new byte[chars.length];

        for (int i = 0; i < chars.length; i++) {
            bytes[i] = (byte) chars[i];
        }
        try {
            Files.write(file.toPath(), bytes, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class ReadAssertionSubscriber extends BaseAssertionSubscriber<ByteBuffer, String> {

        @Override
        public void onSubscribe(Subscription s) {
            super.onSubscribe(s);
            s.request(request);
        }

        @Override
        public void onNext(ByteBuffer line) {
            super.onNext(line);
            items.add(UTF_8.decode(line).toString());
        }
    }
}