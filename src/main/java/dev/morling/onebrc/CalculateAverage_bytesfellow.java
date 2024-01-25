/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class CalculateAverage_bytesfellow {

    public static final String CPU_CORES_1BRC_ENV_VARIABLE = "CPU_CORES_1BRC";
    private static final byte Separator = ';';

    private static final double SchedulerCpuRatio = 0.4;

    private static final int availableCpu = System.getenv(CPU_CORES_1BRC_ENV_VARIABLE) != null ? Integer.parseInt(System.getenv(CPU_CORES_1BRC_ENV_VARIABLE))
            : Runtime.getRuntime().availableProcessors();

    private static final int SchedulerPoolSize = Math.max((int) (availableCpu * SchedulerCpuRatio), 1);
    private static final int SchedulerQueueSize = Math.min(SchedulerPoolSize * 3, 12);
    private static final int PartitionsNumber = Math.max((availableCpu - SchedulerPoolSize), 1);
    private static final int PartitionExecutorQueueSize = 1000;

    private static final int InputStreamBlockSize = 4096;
    private static final int InputStreamReadBufferLen = 100 * InputStreamBlockSize;

    static class Partition {

        private static final AtomicInteger cntr = new AtomicInteger(-1);
        private final Map<Station, MeasurementAggregator> partitionResult = new HashMap<>(10000); // as per requirement we have not more than 10K keys
        private final AtomicInteger leftToExecute = new AtomicInteger(0);

        private final String name = "partition-" + cntr.incrementAndGet();

        private final Executor executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(PartitionExecutorQueueSize) { // some limit to avoid OOM
                    @Override
                    public boolean offer(Runnable runnable) {
                        try {
                            put(runnable); // block if limit was exceeded
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return true;
                    }
                }, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName(name);
            return t;
        });

        public void scheduleToProcess(ByteBuffer slice, List<LineParams> lines) {

            if (!lines.isEmpty()) {
                leftToExecute.incrementAndGet();
                executor.execute(
                        () -> {
                            for (int i = 0; i < lines.size(); i++) {
                                LineParams lineParams = lines.get(i);

                                Measurement measurement = getMeasurement(slice, lineParams);

                                MeasurementAggregator measurementAggregator = partitionResult.get(measurement.station);
                                if (measurementAggregator == null) {
                                    partitionResult.put(new Station(measurement.station), new MeasurementAggregator().withMeasurement(measurement));
                                } else {
                                    measurementAggregator.withMeasurement(measurement);
                                }
                            }

                            leftToExecute.decrementAndGet();
                        });
            }

        }

        public void materializeNames() {
            partitionResult.keySet().forEach(Station::materializeName);
        }

        public Map<Station, MeasurementAggregator> getResult() {
            return partitionResult;
        }

        public boolean allTasksCompleted() {
            return leftToExecute.get() == 0;
        }

    }

    record LineParams(int start, int length) {
    }

    static class Partitioner {

        private final List<Partition> allPartitions = new ArrayList<>();
        private final int partitionsSize;

        AtomicInteger jobsScheduled = new AtomicInteger(0);

        final Executor scheduler = new ThreadPoolExecutor(SchedulerPoolSize, SchedulerPoolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(SchedulerQueueSize) { // some limit to avoid OOM

                    @Override
                    public Runnable take() throws InterruptedException {
                        return super.take();
                    }

                    @Override
                    public boolean offer(Runnable runnable) {
                        try {
                            put(runnable); // preventing unlimited scheduling due to possible OOM
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return true;
                    }
                }, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("scheduler");
            return t;
        });

        Partitioner(int partitionsSize) {
            IntStream.range(0, partitionsSize).forEach((i) -> allPartitions.add(new Partition()));
            this.partitionsSize = partitionsSize;
        }

        private int partitionsSize() {
            return partitionsSize;
        }

        void processSlice(ByteBuffer slice) {

            jobsScheduled.incrementAndGet();

            scheduler.execute(() -> {
                List<List<LineParams>> partitionedLines = new ArrayList<>(partitionsSize());
                IntStream.range(0, partitionsSize()).forEach((p) -> partitionedLines.add(new ArrayList<>(slice.limit() / 116 / 2))); // 116 is the max line len incl new line char

                int start = 0;
                int i = 0;
                int startCharLen = 0;
                while (i < slice.limit()) {

                    if (slice.get(i) == '\n' || i == (slice.limit() - 1)) {

                        int lineLength = i - start;
                        LineParams lineParams = new LineParams(start, lineLength);

                        int partitioningCode = getPartitioningCode(slice, start, getUtf8CharNumberOfBytes(slice.get(start)));
                        int partition = computePartition(partitioningCode);

                        partitionedLines.get(partition).add(lineParams);
                        start = i + 1;

                    }

                    i++;
                }

                processPartitionedBatch(slice, partitionedLines);

                jobsScheduled.decrementAndGet();
            });

        }

        private void processPartitionedBatch(ByteBuffer slice, List<List<LineParams>> partitionedLines) {
            for (int i = 0; i < partitionedLines.size(); i++) {
                allPartitions.get(i).scheduleToProcess(slice, partitionedLines.get(i));
            }
        }

        private int computePartition(int code) {
            return Math.abs(code % partitionsSize());
        }

        private static int getPartitioningCode(ByteBuffer line, int start, int utf8CharNumberOfBytes) {
            // seems good enough
            if (utf8CharNumberOfBytes == 4) {
                return line.get(start) + line.get(start + 1) + line.get(start + 2) + line.get(start + 3);
            } else if (utf8CharNumberOfBytes == 3) {
                return line.get(start) + line.get(start + 1) + line.get(start + 2);
            } else if (utf8CharNumberOfBytes == 2) {
                return line.get(start) + line.get(start + 1);
            } else {
                return line.get(start);
            }
        }

        SortedMap<Station, MeasurementAggregator> getAllResults() {
            allPartitions.parallelStream().forEach(Partition::materializeNames);
            SortedMap<Station, MeasurementAggregator> result = new TreeMap<>();
            allPartitions.forEach((p) -> result.putAll(p.getResult()));
            return result;
        }

        public boolean allTasksCompleted() {
            return allPartitions.stream().allMatch(Partition::allTasksCompleted);
        }

    }

    class UnsafeMap<K, V> implements Map<K, V> {

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsKey(Object key) {
            return false;
        }

        @Override
        public boolean containsValue(Object value) {
            return false;
        }

        @Override
        public V get(Object key) {
            return null;
        }

        @Override
        public V put(K key, V value) {
            return null;
        }

        @Override
        public V remove(Object key) {
            return null;
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {

        }

        @Override
        public void clear() {

        }

        @Override
        public Set<K> keySet() {
            return null;
        }

        @Override
        public Collection<V> values() {
            return null;
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return null;
        }
    }

    private static final String FILE = "./measurements.txt";

    public static class Station implements Comparable<Station> {

        private final ByteBuffer inputSlice;
        private final int hash;

        private final int startIdx;
        private final int len;

        private volatile String nameAsString;
        // private long[] compactedString;

        private boolean dontSLice = false;

        public Station(ByteBuffer inputSlice, int startIdx, int len) {
            this.inputSlice = inputSlice;
            this.startIdx = startIdx;
            this.len = len;

            this.hash = hashcodeFast();

        }

        public Station(Station from) {
            byte[] str = new byte[from.len];
            from.inputSlice.get(from.startIdx, str, 0, from.len);
            this.inputSlice = ByteBuffer.wrap(str);

            this.startIdx = 0;
            this.len = from.len;

            this.hash = from.hash;
            this.dontSLice = true;
            // this.compactedString = from.compactedString;
        }

        private int hashCode109() {
            if (len == 0)
                return 0;
            int h = inputSlice.get(startIdx);
            for (int i = startIdx + 1; i < startIdx + len; i++) {
                h = (h << 7) + inputSlice.get(i);
            }
            h *= 109;
            return h;
        }

        private int hashcodeFast() {
            if (len == 0) {
                return 0;
            } else if (len == 1) {
                return inputSlice.get(startIdx) * 109;
            } else if (len == 2) {
                return inputSlice.get(startIdx + 1) * 109 * 109 + inputSlice.get(startIdx);
            } else if (len == 3) {
                return inputSlice.get(startIdx + 2) * 109 * 109 * 109 + inputSlice.get(startIdx + 1) * 109 * 109 + inputSlice.get(startIdx);
            } else {
                return inputSlice.get(startIdx + 3) * 109 * 109 * 109 * 109 + inputSlice.get(startIdx + 2) * 109 * 109 * 109 + inputSlice.get(startIdx + 1) * 109 * 109
                        + inputSlice.get(startIdx);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Station station = (Station) o;

            if (len != station.len) {
                return false;
            }

            return (dontSLice ? inputSlice : inputSlice.slice(startIdx, len))
                    .equals(station.dontSLice ? station.inputSlice : station.inputSlice.slice(station.startIdx, station.len));

        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public int compareTo(Station o) {
            return materializeName().compareTo(o.materializeName()); //
        }

        public String materializeName() {
            if (nameAsString == null && len >= 0) {
                byte[] nameForMaterialization = new byte[len];
                inputSlice.get(startIdx, nameForMaterialization, 0, len);
                nameAsString = new String(nameForMaterialization, StandardCharsets.UTF_8);
            }

            return nameAsString;
        }

        @Override
        public String toString() {
            return materializeName();
        }
    }

    private record Measurement(Station station, long value) {
    }

    private record ResultRow(long min, long sum, long count, long max) {

        public String toString() {
            return fakeDouble(min) + "/" + round((double) sum / (double) count / 10.0) + "/" + fakeDouble(max);
        }

        private String fakeDouble(long value) {
            long positiveValue = value < 0 ? -value : value;
            long wholePart = positiveValue / 10;
            String positiveDouble = wholePart + "." + (positiveValue - wholePart * 10);


            return (value < 0 ? "-" : "") + positiveDouble;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

    }

    public static class MeasurementAggregator {
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long sum;
        private long count;

        MeasurementAggregator withMeasurement(Measurement m) {

            min = Math.min(min, m.value);
            max = Math.max(max, m.value);
            sum += m.value;
            count++;

            return this;
        }

        @Override
        public String toString() {
            return new ResultRow(min, sum, count, max).toString();
        }

    }

    private static long parseToLongIgnoringDecimalPoint(ByteBuffer slice, int startIndex, int len) {
        long value = 0;

        int start = startIndex;
        if (slice.get(startIndex) == '-') {
            start = startIndex + 1;
        }

        for (int i = start; i < startIndex + len; i++) {
            if (slice.get(i) == '.') {
                continue;
            }

            if (i > 0) {
                value = multipleByTen(value); // *= 10;
            }
            value += digitAsLong(slice, i);
        }

        return start > startIndex ? -value : value;
    }

    private static long multipleByTen(long value) {
        return (value << 3) + (value << 1);
    }

    private static long digitAsLong(ByteBuffer digits, int position) {
        return (digits.get(position) - 48);
    }

    public static void main(String[] args) throws IOException {

        Partitioner partitioner = new Partitioner(PartitionsNumber);

        try (FileChannel fc = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            parseStreamWithBytes(fc, InputStreamReadBufferLen, partitioner::processSlice);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        showResults(partitioner);

    }

    static void parseStreamWithBytes(FileChannel inputStream, int bufferLen, Consumer<ByteBuffer> sliceConsumer) throws IOException {

        // byte[] byteArray = new byte[bufferLen];
        int lenToRead = bufferLen;

        int readLen;

        ByteBuffer buffer = ByteBuffer.allocate(bufferLen);
        byte[] tail = null;

        while ((readLen = inputStream.read(buffer)) > -1) {
            if (readLen == 0) {
                continue;
            }

            if (tail != null) {
                buffer = ByteBuffer
                        .allocate(tail.length + buffer.limit())
                        .put(0, tail, 0, tail.length)
                        .put(tail.length, buffer, 0, buffer.limit());
            }

            int traverseLen = buffer.limit(); // Math.min(readLen, bufferLen);
            int positionAfterLastLineBreak = traverseLen;

            for (int j = traverseLen - 1; j >= 0; j--) {
                if (buffer.get(j) == '\n') {
                    positionAfterLastLineBreak = j + 1;
                    break;
                }
            }

            int sliceSize = traverseLen / SchedulerPoolSize; // todo:

            int currentSliceStart = 0;

            int currentSliceEnd = Math.min(sliceSize, positionAfterLastLineBreak - 1);

            while (currentSliceStart < positionAfterLastLineBreak && currentSliceEnd < positionAfterLastLineBreak) {
                if (buffer.get(currentSliceEnd) == '\n') {
                    int sliceLen = currentSliceEnd - currentSliceStart + 1;
                    ByteBuffer slice = buffer.slice(currentSliceStart, sliceLen);
                    // var tmp = new byte[sliceLen];
                    // slice.get(0, tmp, 0, sliceLen);

                    // System.out.println("|" + new String(tmp, StandardCharsets.UTF_8) + "|");

                    sliceConsumer.accept(slice);

                    currentSliceStart = currentSliceEnd + 1;
                    currentSliceEnd = Math.min(currentSliceStart + sliceSize, positionAfterLastLineBreak - 1);

                } else {
                    currentSliceEnd++;
                }

            }

            if (currentSliceStart < traverseLen && positionAfterLastLineBreak < traverseLen) {
                // some tail left, carry it over to the next read
                int len = traverseLen - currentSliceStart;

                tail = new byte[len];
                buffer.get(currentSliceStart, tail, 0, len);

                lenToRead = bufferLen - len;
            } else {
                // offset = 0;
                lenToRead = bufferLen;
                tail = null;
            }
            // buffer.clear();
            buffer = ByteBuffer.allocate(bufferLen);
        }
    }

    static int getUtf8CharNumberOfBytes(byte firstByteOfChar) {
        int masked = firstByteOfChar & 0b11111000;
        if (masked == 0b11110000) {
            return 4;
        } else if (masked == 0b11100000) {
            return 3;
        } else if (masked == 0b11000000) {
            return 2;
        } else {
            return 1;
        }
    }

    static void showResults(Partitioner partitioner) {

        CountDownLatch c = new CountDownLatch(1);
        partitioner.scheduler.execute(() -> {

            try {
                // check if any unprocessed slices
                while (partitioner.jobsScheduled.get() > 0) {
                }

                // check if anything left in partitions
                while (!partitioner.allTasksCompleted()) {
                }

                SortedMap<Station, MeasurementAggregator> result = partitioner.getAllResults();
                System.out.println(result); // output aggregated measurements according to the requirement
            } catch (Exception e) {
                System.out.println(e);
            }
            c.countDown();
        });

        try {
            c.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private static Measurement getMeasurement(ByteBuffer slice, LineParams lineParams) {
        int idx = lastIndexOfSeparator(slice, lineParams);
        return new Measurement(
                new Station(slice, lineParams.start, idx - lineParams.start),
                parseToLongIgnoringDecimalPoint(slice, idx + 1, lineParams.start + lineParams.length - (idx + 1)));
    }

    private static int lastIndexOfSeparator(ByteBuffer slice, LineParams lineParams) {
        // hacky - we know that from the end of the line we have only
        // single byte characters
        // -2 is also hacky since we expect a particular format at the end of the line

        int lastIdx = lineParams.start + lineParams.length - 1;

        if (slice.get(lastIdx - 2) == Separator) {
            return lastIdx - 2;
        } else if (slice.get(lastIdx - 3) == Separator) {
            return lastIdx - 3;
        } else if (slice.get(lastIdx - 4) == Separator) {
            return lastIdx - 4;
        } else if (slice.get(lastIdx - 5) == Separator) {
            return lastIdx - 5;
        }

        return -1;
    }

}
