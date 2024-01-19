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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class CalculateAverage_bytesfellow {

    public static int partitionsNumber = Runtime.getRuntime().availableProcessors() > 2 ? Runtime.getRuntime().availableProcessors() - 1 : 1;

    public static int PartitionCapacity = 10000;
    private final static byte Separator = ';';

    static class Partition {

        private static AtomicInteger cntr = new AtomicInteger(-1);
        private final Map<Station, MeasurementAggregator> partitionResult = new ConcurrentHashMap<>();
        private final byte[][] queue = new byte[PartitionCapacity][];
        private volatile int top = 0;

        private final AtomicInteger leftToExecute = new AtomicInteger(0);

        private String name = "partition-" + cntr.incrementAndGet();

        int partitionExecutorQueueSize = 1000;

        private final Object lock = new Object();

        private final Executor executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(partitionExecutorQueueSize) { // some limit to avoid OOM
                    @Override
                    public boolean offer(Runnable runnable) {
                        try {
                            put(runnable); // block if limit was exceeded
                        }
                        catch (InterruptedException e) {
                            // swallow exception
                        }
                        return true;
                    }
                }, r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    t.setName(name);
                    return t;
                });

        public void add(byte[] line) {
            queue[top++] = line;
        }

        public void addAll(List<byte[]> lines) {
            processQueued(lines);
            // queue[top++] = line;
            /*
             * synchronized (lock) {
             * lines.forEach((line) -> {
             * queue[top++] = line;
             * 
             * if (top == PartitionCapacity) {
             * processQueued();
             * }
             * });
             * }
             */
        }

        public void processQueued(List<byte[]> lines) {

            if (!lines.isEmpty()) {
                leftToExecute.incrementAndGet();
                ForkJoinPool.commonPool().execute(
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

        int scheduleQueueSize = 100;

        AtomicInteger jobsScheduled = new AtomicInteger(0);

        final Executor scheduler = new ThreadPoolExecutor(5, 5,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(scheduleQueueSize) { // some limit to avoid OOM

                    @Override
                    public boolean offer(Runnable runnable) {
                        try {
                            put(runnable); // preventing unlimited scheduling due to possible OOM
                        }
                        catch (InterruptedException e) {
                            // swallow exception
                        }
                        return true;
                    }
                }, r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    t.setName("scheduler");
                    return t;
                });

        Partitioner(int partitionsNumber) {
            for (int i = 0; i < partitionsNumber; i++) {
                allPartitions.add(new Partition());
            }

        }

        public void schedule(byte[][] toProcess, int toProcessLen) {

            jobsScheduled.incrementAndGet();
            scheduler.execute(() -> {
                processBatch(toProcess, toProcessLen);
                jobsScheduled.decrementAndGet();
            });

        }

        private void processBatch(byte[][] toProcess, int len) {

            Map<Integer, List<byte[]>> collect = Arrays.stream(toProcess)
                    .map((line) -> Map.entry(getPartitionNumber(line), line))
                    .collect(Collectors.groupingBy(Map.Entry::getKey,
                            Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

            /* .parallelStream() */
            collect.entrySet().forEach((entry) -> allPartitions.get(entry.getKey()).addAll(entry.getValue()));

            /*
             * for (int i = 0; i < len; i++) {
             * add(toProcess[i]);
             * }
             */

            // processQueuedInAllPartitions();

        }

        void processQueuedInAllPartitions() {
            // allPartitions.forEach(Partition::processQueued);
        }

        public void add(byte[] line) {
            int partitionNumber = getPartitionNumber(line);

            allPartitions.get(partitionNumber).add(line);
        }

        private int getPartitionNumber(byte[] line) {

            int code = getCode(line);

            return computePartition(code);
        }

        private int computePartition(int code) {
            return Math.abs(code % allPartitions.size());
        }

        private static int getCode(byte[] line) {
            int utf8CharNumberOfBytes = getUtf8CharNumberOfBytes(line[0]);

            return line[utf8CharNumberOfBytes - 1];

            /*
             * long value = 0;
             * for (int i = 0; i < utf8CharNumberOfBytes; i++) {
             * value += ((long) line[i] & 0xffL) << (8 * i);
             * }
             * return (int) value;
             */
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
        private int hash = -1;

        private volatile String nameAsString;

        public Station(byte[] inputLine, int len) {
            this.name = new byte[len];
            System.arraycopy(inputLine, 0, name, 0, len);
            this.len = len;
            this.hash = Arrays.hashCode(name);
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

    private static long parseToLongIgnoringDecimalPoint(byte[] digits, int startIndex) {
        long value = 0;

        int start = startIndex;
        if (digits[startIndex] == '-') {
            start = startIndex + 1;
        }

        for (int i = start; i < digits.length; i++) {
            if (digits[i] == '.') {
                continue;
            }

            if (i > 0) {
                value *= 10;
            }
            value += asLong(digits, i);
        }
        return start > startIndex ? -value : value;
    }

    private static long asLong(byte[] digits, int position) {
        return (digits[position] - 48);
    }

    public static void main(String[] args) throws IOException {

        Partitioner partitioner = new Partitioner(partitionsNumber);

        try (FileInputStream fileInputStream = new FileInputStream(FILE)) {
            parseStream(fileInputStream, 5000000, getHandler(partitioner));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        showResultsAndWait(partitioner);

    }

    private static BiConsumer<byte[][], Integer> getHandler(Partitioner partitioner) {
        return (byte[][] buffer, Integer ptr) -> handleParsedLines(buffer, ptr, partitioner);
    }

    static void parseStream(InputStream inputStream, int bufferLen, BiConsumer<byte[][], Integer> consumer) throws IOException {
        byte[][] buffer = new byte[PartitionCapacity][];// new String[PartitionCapacity];
        int ptr = 0;

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
            while (i < traverseLen) {
                int charSizeInBytes = getUtf8CharNumberOfBytes(byteArray[i]);

                if (charSizeInBytes == 1) {
                    // single byte char
                    // check for the new line
                    if (byteArray[i] == 0x0a || byteArray[i] == 0x0d) {

                        /*
                         * TODO:
                         * - check if the new line was on the first position (empty line)
                         * - skip \r check
                         * if(i==0){
                         * nameIndexStart++;
                         * continue;
                         * }
                         */

                        // string is in [nameIndexStart, i-1]
                        int strBufferLen = i - nameIndexStart;
                        var strBuffer = new byte[strBufferLen];
                        System.arraycopy(byteArray, nameIndexStart, strBuffer, 0, strBufferLen);
                        nameIndexStart = i + 1;
                        // String parsedString = new String(strBuffer, StandardCharsets.UTF_8);

                        buffer[ptr++] = strBuffer;// parsedString;

                        if (ptr == PartitionCapacity) {
                            consumer.accept(buffer, ptr);

                            ptr = 0;
                        }

                    }
                    i++;
                }
                else {
                    i += charSizeInBytes;
                }

            }

            if (nameIndexStart < traverseLen - 1) {
                // we have some data left in the buffer
                // and it wasn't terminated with the new line

                if (nameIndexStart > 0) {
                    // if the remaining part wasn't already at the beginning of the string,
                    // then copy over to the beginning and read the next portion
                    int lengthOfRemainingBytes = traverseLen - nameIndexStart;
                    System.arraycopy(byteArray, nameIndexStart, byteArray, 0, lengthOfRemainingBytes);
                    offset = lengthOfRemainingBytes;
                    lenToRead = bufferLen - lengthOfRemainingBytes;
                }
            }
            else {
                offset = 0;
                lenToRead = bufferLen;
                nameIndexStart = 0;
            }

        }

        if (ptr > 0) {
            consumer.accept(buffer, ptr);
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
            partitioner.processQueuedInAllPartitions();

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

    private static void handleParsedLines(byte[][] buffer, int toProcessLen, Partitioner partitioner) {
        // String[] toProcess = new String[toProcessLen];
        byte[][] toProcess = new byte[toProcessLen][];
        System.arraycopy(buffer, 0, toProcess, 0, toProcessLen);

        partitioner.schedule(toProcess, toProcessLen);

    }

    private static Measurement getMeasurement(byte[] line) {
        int idx = lastIndexOf(line);

        long temperature = parseToLongIgnoringDecimalPoint(line, idx + 1);

        // String substring = line.substring(0, idx);

        Measurement measurement = new Measurement(new Station(line, idx), temperature);
        return measurement;
    }

    private static int lastIndexOf(byte[] line) {
        // we know that from the end of the line we have only
        // single byte chars
        for (int i = line.length - 1 - 2; i >= 0; i--) { // -2 is hacky
            if (line[i] == Separator) {
                return i;
            }
        }
        return -1;
    }

}
