# ReactiveIO
Reactive file readers based on [NIO](https://en.wikipedia.org/wiki/New_I/O_(Java)) and [Flow API](https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/Flow.html) that could be integrated with [Flux](https://projectreactor.io/docs/core/release/api/reactor/adapter/JdkFlowAdapter.html), [RxJava](https://github.com/akarnokd/RxJava2Jdk9Interop).

[![Build Status](https://travis-ci.org/Alexey911/TravisTest.png?branch=master)](https://travis-ci.org/Alexey911/ReactiveIO)
[![Coverage Status](https://coveralls.io/repos/github/Alexey911/ReactiveIO/badge.svg?branch=master)](https://coveralls.io/github/Alexey911/ReactiveIO?branch=master)

[LineReader](https://github.com/Alexey911/ReactiveIO/blob/master/src/main/java/com/zhytnik/reactive/io/LineReader.java) provides alternative for `Stream<String>` from [Files.lines(Path path)](https://docs.oracle.com/javase/9/docs/api/java/nio/file/Files.html#lines-java.nio.file.Path-) and reads lines by `ByteBuffers`. Almost always `LineReader` consumes only 32KB of heap memory and its consumption isn't depend on file's size (uses additional memory only for lines that are greater than 32768 characters). 

Also there's [FileReader](https://github.com/Alexey911/ReactiveIO/blob/master/src/main/java/com/zhytnik/reactive/io/FileReader.java) for simple reactive reading. 

### Examples

Printing all lines
```java
new LineReader(Paths.get("resource.txt")).subscribe(new Flow.Subscriber<>() {
    @Override
    public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }

    @Override
    public void onNext(ByteBuffer line) {
        System.out.println(new String(line.array(), line.position(), line.remaining(), UTF_8));
    }

    @Override
    public void onError(Throwable t) { t.printStackTrace(); }

    @Override
    public void onComplete() { }
});
```

Getting count of non-empty lines
```java
new LineReader(Paths.get("resource.txt")).subscribe(new Flow.Subscriber<>() {

    int count;

    @Override
    public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }

    @Override
    public void onNext(ByteBuffer line) { if (line.hasRemaining()) ++count; }

    @Override
    public void onError(Throwable t) { t.printStackTrace(); }

    @Override
    public void onComplete() { System.out.println("The file contains " + count + " non-empty lines."); }
});
```

### ByteBuffer vs String
When there's no need to process data via `String` representation or it's required to know only existance of a line (e.g. getting line count), using of `LineReader` gives significant speed up.

On my machine ([Core i5-3317U](https://ark.intel.com/ru/products/65707/Intel-Core-i5-3317U-Processor-3M-Cache-up-to-2_60-GHz), Ubuntu 14.04, Oracle JDK 9.0.4) [getting line count](https://github.com/Alexey911/ReactiveIO/issues/5) from 8MB file took:
- 12.625 ± 0.351 ms (by `LineReader`)
- 48.354 ± 1.890 ms (by `Stream<String>`)
- 24.316 ± 0.696 ms (by parallel `Stream<String>`)

### License

This project is licensed under [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
