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
    public void stressTest() {
        read(
                '0', '1', '2', '3', '\n',
                '4', '5', '\r', '\n',
                '6', '7', '\r',
                '8'
        );

        assertThat(subscriber.items).containsSequence("0123", "45", "67", "8");
    }

    @Test
    public void parsesDifferentLineEnds() {
        read(
                '0', '\r',
                '2', '\n',
                '6', '\r',
                '7', '\r', '\n',
                '8'
        );

        assertThat(subscriber.items).containsSequence("0", "2", "6", "7", "8");
    }

    @Test
    public void parsesEmptyLine() {
        read();

        assertThat(subscriber.items).isEmpty();
    }

    @Test
    public void parsesSingleCharLine() {
        read('\r');

        assertThat(subscriber.items).containsSequence("8");
    }

    @Test
    public void parsesSingleLine() {
        read('4', '\r', '\n');

        assertThat(subscriber.items).containsSequence("4");
    }

    @Test
    public void parsesFewContinuousEmptyLines() {
        read(
                '0', '\n',
                '\r', '\n',
                '6', '\r',
                '8'
        );

        assertThat(subscriber.items).containsSequence("0", "", "6", "8");
    }

    @Test
    public void parsesRepeatableLines() {
        read(
                '0', '\n',
                '1', '\n'
        );

        assertThat(subscriber.items).containsSequence("0", "1");
    }

    @After
    public void validate() {
        subscriber.validate();
    }

    void read(char... vals) {
        writeToFile(vals);
        subscriber.request = MAX_VALUE;
        reader.subscribe(subscriber);
    }

    void writeToFile(char... vals) {
        final byte[] out = new byte[vals.length];

        for (int i = 0; i < vals.length; i++) out[i] = (byte) vals[i];

        try {
            Files.write(file.toPath(), out, StandardOpenOption.APPEND);
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