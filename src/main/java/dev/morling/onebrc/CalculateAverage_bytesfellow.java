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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class CalculateAverage_bytesfellow {

    private static final byte Separator = ';';

    private static final double SchedulerCpuRatio = 0.3;
    private static final double PartitionerCpuRatio = 0.7;

    private static final int availableCpu = Runtime.getRuntime().availableProcessors();

    private static final int SchedulerPoolSize = Math.max((int) (availableCpu * SchedulerCpuRatio), 1);
    private static final int SchedulerQueueSize = Math.min(SchedulerPoolSize * 3, 12);
    private static final int PartitionsNumber = Math.max((int) (availableCpu * PartitionerCpuRatio), 1);
    private static final int PartitionExecutorQueueSize = 1000;
    private static final int InputStreamReadBufferLen = 1000000;

    static class Partition {

        private static AtomicInteger cntr = new AtomicInteger(-1);
        private final Map<Station, MeasurementAggregator> partitionResult = new HashMap<>();
        private final AtomicInteger leftToExecute = new AtomicInteger(0);

        private String name = "partition-" + cntr.incrementAndGet();

        private final Object lock = new Object();

        private final Executor executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(PartitionExecutorQueueSize) { // some limit to avoid OOM
                    @Override
                    public boolean offer(Runnable runnable) {
                        try {
                            put(runnable); // block if limit was exceeded
                        }
                        catch (InterruptedException e) {
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

        public void scheduleToProcess(List<byte[]> lines) {

            if (!lines.isEmpty()) {
                leftToExecute.incrementAndGet();
                executor.execute(
                        () -> {
                            for (byte[] line : lines) {

                                Measurement measurement = getMeasurement(line);
                                partitionResult.compute(measurement.station,
                                        (k, v) -> v == null ? new MeasurementAggregator().withMeasurement(measurement) : v.withMeasurement(measurement));

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

    static class Partitioner {

        private final List<Partition> allPartitions = new ArrayList();
        private final int partitionsSize;

        AtomicInteger jobsScheduled = new AtomicInteger(0);

        final Executor scheduler = new ThreadPoolExecutor(SchedulerPoolSize, SchedulerPoolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(SchedulerQueueSize) { // some limit to avoid OOM

                    @Override
                    public Runnable take() throws InterruptedException {
                        return super.take();
                    }

                    @Override
                    public boolean offer(Runnable runnable) {
                        try {
                            put(runnable); // preventing unlimited scheduling due to possible OOM
                        }
                        catch (InterruptedException e) {
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

        void processSlice(byte[] slice) {

            jobsScheduled.incrementAndGet();

            scheduler.execute(() -> {

                List<List<byte[]>> partitionedLines = new ArrayList<>(partitionsSize());
                IntStream.range(0, partitionsSize()).forEach((p) -> partitionedLines.add(new ArrayList<>()));

                // ArrayList<byte[]> lines = new ArrayList<>();
                int start = 0;
                int i = 0;
                while (i < slice.length) {

                    int utf8CharNumberOfBytes = getUtf8CharNumberOfBytes(slice[i]);

                    if (slice[i] == '\n' || i == (slice.length - 1)) {

                        int lineLength = i - start + (i == (slice.length - 1) ? 1 : 0);
                        byte[] line = getLine(slice, lineLength, start);
                        start = i + 1;

                        int partition = computePartition(getPartitioningCode(line, utf8CharNumberOfBytes));

                        partitionedLines.get(partition).add(line);
                    }

                    i += utf8CharNumberOfBytes;

                }

                processPartitionedBatch(partitionedLines);

                jobsScheduled.decrementAndGet();
            });

        }

        private static byte[] getLine(byte[] slice, int lineLength, int start) {
            byte[] line = new byte[lineLength];
            System.arraycopy(slice, start, line, 0, lineLength);
            return line;
        }

        private void processPartitionedBatch(List<List<byte[]>> partitionedLines) {
            for (int i = 0; i < partitionedLines.size(); i++) {
                allPartitions.get(i).scheduleToProcess(partitionedLines.get(i));
            }
        }

        private int computePartition(int code) {
            return Math.abs(code % partitionsSize());
        }

        private static int getPartitioningCode(byte[] line, int utf8CharNumberOfBytes) {

            return line[utf8CharNumberOfBytes - 1] + (utf8CharNumberOfBytes > 1 ? line[utf8CharNumberOfBytes - 2] : (byte) 0);
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

    private static final String FILE = "./measurements.txt";

    private static class Station implements Comparable<Station> {

        private final byte[] name;
        private final int len;
        private final int hash;

        private volatile String nameAsString;

        public Station(byte[] inputLine, int len) {
            this.name = new byte[len];
            System.arraycopy(inputLine, 0, name, 0, len);
            this.len = len;
            this.hash = hashCode109(name);
        }

        int hashCode109(byte[] value) {
            if (value.length == 0)
                return 0;
            int h = value[0];
            for (int i = 1; i < value.length; i++) {
                h = h * 109 + value[i];
            }
            h *= 109;
            return h;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Station station = (Station) o;
            return Arrays.equals(name, station.name);
        }

        @Override
        public int hashCode() {

            return hash;
        }

        @Override
        public int compareTo(Station o) {
            return materializeName().compareTo(o.materializeName()); //
            // return Arrays.compare(name, o.name); // name.compareTo(o.name);
        }

        public String materializeName() {
            if (nameAsString == null) {
                nameAsString = new String(name, StandardCharsets.UTF_8);
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

    ;

    private static class MeasurementAggregator {
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

    private static long parseToLongIgnoringDecimalPoint(byte[] lineWithDigits, int startIndex) {
        long value = 0;

        int start = startIndex;
        if (lineWithDigits[startIndex] == '-') {
            start = startIndex + 1;
        }

        for (int i = start; i < lineWithDigits.length; i++) {
            if (lineWithDigits[i] == '.') {
                continue;
            }

            if (i > 0) {
                value = (value << 3) + (value << 1); // *= 10;
            }
            value += asLong(lineWithDigits, i);
        }
        return start > startIndex ? -value : value;
    }

    private static long asLong(byte[] digits, int position) {
        return (digits[position] - 48);
    }

    public static void main(String[] args) throws IOException {

        Partitioner partitioner = new Partitioner(PartitionsNumber);

        try (FileInputStream fileInputStream = new FileInputStream(FILE)) {
            parseStreamNew(fileInputStream, InputStreamReadBufferLen, partitioner::processSlice);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        showResultsAndWait(partitioner);

    }

    static void parseStreamNew(InputStream inputStream, int bufferLen, Consumer<byte[]> sliceConsumer) throws IOException {

        byte[] byteArray = new byte[bufferLen];
        int offset = 0;
        int lenToRead = bufferLen;

        int readLen;

        while ((readLen = inputStream.read(byteArray, offset, lenToRead)) > -1) {
            if (readLen == 0) {
                continue;
            }

            int i = 0;
            int nameIndexStart = 0;
            int traverseLen = Math.min(offset + readLen, bufferLen); // fix this
            int linesReadSize = traverseLen;

            // splice byteArray to several parts
            // parse each part in parallel
            // remaining piece copy to the beginning
            // continue reading

            // find the first end of line from the end of the buffer

            for (int j = traverseLen - 1; j >= 0; j--) {

                if (byteArray[j] == '\n') {
                    linesReadSize = j + 1;
                    break;
                }

            }

            if (linesReadSize == traverseLen) {
                // todo: end of line was not found in a slice?
            }

            int numSlices = SchedulerPoolSize;
            int sliceSize = linesReadSize / numSlices;

            ArrayList<byte[]> slices = new ArrayList<>();

            int s = 0, len = -1;
            int j = Math.min(sliceSize, linesReadSize - 1);

            while (s < linesReadSize && j < linesReadSize) {
                if (byteArray[j] == '\n') {
                    len = j - s;
                    byte[] slice = new byte[len];
                    System.arraycopy(byteArray, s, slice, 0, len);
                    slices.add(slice);
                    sliceConsumer.accept(slice);

                    s = j + 1;
                    j = Math.min(s + sliceSize, linesReadSize - 1);

                }
                else {
                    j++;
                }
            }

            if (s < traverseLen && linesReadSize < traverseLen) {
                len = traverseLen - s;
                System.arraycopy(byteArray, s, byteArray, 0, len);
                offset = len;
                lenToRead = bufferLen - len;
            }
            else {
                offset = 0;
                lenToRead = bufferLen;
            }

            // slices.forEach((slice) -> sliceConsumer.accept(slice));

        }
    }

    static int getUtf8CharNumberOfBytes(byte firstByteOfChar) {
        if ((firstByteOfChar & 0b11111000) == 0b11110000) {
            // four bytes char
            return 4;
        }
        else if ((firstByteOfChar & 0b11110000) == 0b11100000) {
            // three bytes char
            return 3;
        }
        else if ((firstByteOfChar & 0b11100000) == 0b11000000) {
            // two bytes char
            return 2;
        }
        else {
            return 1;
        }
    }

    static void showResultsAndWait(Partitioner partitioner) {

        CountDownLatch c = new CountDownLatch(1);
        partitioner.scheduler.execute(() -> {

            while (partitioner.jobsScheduled.get() > 0) {
            }

            while (!partitioner.allTasksCompleted()) {
            }
            SortedMap<Station, MeasurementAggregator> result = partitioner.getAllResults();

            System.out.println(result); // output aggregated measurements according to the requirement
            c.countDown();
        });

        try {
            c.await();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private static Measurement getMeasurement(byte[] line) {
        int idx = lastIndexOfSeparator(line);

        long temperature = parseToLongIgnoringDecimalPoint(line, idx + 1);

        Measurement measurement = new Measurement(new Station(line, idx), temperature);
        return measurement;
    }

    private static int lastIndexOfSeparator(byte[] line) {
        // hacky - we know that from the end of the line we have only
        // single byte chars
        // -2 is also hacky since we expect partcular format
        for (int i = line.length - 1 - 2; i >= 0; i--) {
            if (line[i] == Separator) {
                return i;
            }
        }
        return -1;
    }

}
