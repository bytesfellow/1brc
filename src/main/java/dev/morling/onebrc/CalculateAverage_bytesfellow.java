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

import java.io.*;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CalculateAverage_bytesfellow {

    public static int PartitionCapacity = 50000;

    static class Partition {

        private static AtomicInteger cntr = new AtomicInteger(-1);
        private final Map<Station, MeasurementAggregator> partition = new HashMap<>();
        private final byte[][] queue = new byte[PartitionCapacity][];// new String[PartitionCapacity];
        private int top = 0;

        private volatile long leftToExecute = 0;

        private String name = "partition-" + cntr.incrementAndGet();

        private final Executor executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(10) { // some limit to avoid OOM
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

        public int add(byte[] line) {
            queue[top++] = line;
            return top;
        }

        public void processQueued() {
            leftToExecute++;

            final int currentTop = top;
            final byte[][] currentQueue = new byte[PartitionCapacity][];
            System.arraycopy(queue, 0, currentQueue, 0, currentTop);

            // clear the queue by just moving the top pointer without deleting actual data
            top = 0;

            executor.execute(
                    () -> {
                        if (currentTop > 0) {
                            for (int j = 0; j < currentTop; j++) {

                                byte[] line = currentQueue[j];

                                Measurement measurement = getMeasurement(line);
                                partition.compute(measurement.station,
                                        (k, v) -> v == null ? new MeasurementAggregator().withMeasurement(measurement) : v.withMeasurement(measurement));

                            }
                        }
                        leftToExecute--;
                    });
        }

        public Map<Station, MeasurementAggregator> getResult() {
            return partition;
        }

        public boolean allTasksCompleted() {
            return leftToExecute == 0;
        }

    }

    static class Partitioner {

        private final List<Partition> newPartitions = new ArrayList(PartitionCapacity);
        final Executor scheduler;

        Partitioner(int partitionsNumber) {
            for (int i = 0; i < partitionsNumber; i++) {
                newPartitions.add(new Partition());
            }

            scheduler = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(100) { // some limit to avoid OOM
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
                        t.setName("scheduler");
                        return t;
                    });

        }

        void processSync() {

            for (int i = 0; i < newPartitions.size(); i++) {
                newPartitions.get(i).processQueued();
            }

        }

        public int add(byte[] line) {
            int partitionNumber = getPartitionNumber(line);

            return newPartitions.get(partitionNumber).add(line);
        }

        private int getPartitionNumber(byte[] line) {
            return Math.abs(line[0] % newPartitions.size());
        }

        SortedMap<Station, MeasurementAggregator> getResult() {
            SortedMap<Station, MeasurementAggregator> result = new TreeMap<>();
            newPartitions.forEach((p) -> result.putAll(p.getResult()));
            return result;
        }

        public boolean allTasksCompleted() {
            return newPartitions.stream().allMatch(Partition::allTasksCompleted);
        }

    }

    private static final String FILE = "./measurements.txt";

    private static class Station implements Comparable<Station> {

        private final byte[] name;
        private final int len;
        private int hash = -1;

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
            return Arrays.compare(name, o.name); // name.compareTo(o.name);
        }

        @Override
        public String toString() {
            return new String(name, StandardCharsets.UTF_8);
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
        int partitionsNumber = Runtime.getRuntime().availableProcessors() > 2 ? Runtime.getRuntime().availableProcessors() - 1 : 1;

        Partitioner partitioner = new Partitioner(partitionsNumber);

        Path path = Paths.get(FILE);
        long size = Files.size(path);

        byte[][] buffer = new byte[PartitionCapacity][];// new String[PartitionCapacity];
        int ptr = 0;

        int bufferLen = 100000; // 16384;
        byte[] byteArray = new byte[bufferLen]; // todo: fix to make it equals to the page size
        int offset = 0;
        int lenToRead = bufferLen;

        try (FileInputStream fileInputStream = new FileInputStream(FILE)) {

            int readLen;

            while ((readLen = fileInputStream.read(byteArray, offset, lenToRead)) > -1) {
                if (readLen == 0) {
                    continue; // todo: double check can this happen
                }

                int i = 0;
                int nameIndexStart = 0;
                int traverseLen = Math.min(offset + readLen, bufferLen); // fix this
                while (i < traverseLen) {
                    if ((byteArray[i] & 0b11111000) == 0b11110000) {
                        // four bytes char
                        i += 4;
                    }
                    else if ((byteArray[i] & 0b11110000) == 0b11100000) {
                        // three bytes char
                        i += 3;
                    }
                    else if ((byteArray[i] & 0b11100000) == 0b11000000) {
                        // two bytes char
                        i += 2;
                    }
                    else {

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
                                handleBuffer(ptr, buffer, partitioner);
                                ptr = 0;
                            }

                        }
                        i++;
                    }

                }

                if (nameIndexStart < traverseLen - 1) {
                    // we have some data left in the buffer
                    // and it wasn't terminated with the new line
                    // copy over to the beginning and read the next portion
                    int lengthOfRemainingBytes = traverseLen - nameIndexStart;
                    System.arraycopy(byteArray, nameIndexStart, byteArray, 0, lengthOfRemainingBytes);
                    offset = lengthOfRemainingBytes;
                    lenToRead = bufferLen - lengthOfRemainingBytes;
                }
                else {
                    offset = 0;
                    lenToRead = bufferLen;
                    nameIndexStart = 0;
                }

                /*
                 * int nameIndexStart = 0;
                 * for (int i = 0; i < bufferLen; i++) {
                 * if (byteArray[i] == 0x0a) {
                 *//*
                    * TODO:
                    * - check if the new line was on the first position (empty line)
                    * - skip \r check
                    * if(i==0){
                    * nameIndexStart++;
                    * continue;
                    * }
                    *//*
                       *
                       * // string is in [nameIndexStart, i-1]
                       * int strBufferLen = i - nameIndexStart;
                       * var strBuffer = new byte[strBufferLen];
                       * System.arraycopy(byteArray, nameIndexStart, strBuffer, 0, strBufferLen);
                       * String parsedString = new String(strBuffer, StandardCharsets.UTF_8);
                       * System.out.println(parsedString);
                       *
                       *
                       * }
                       * }
                       */

                /*
                 * int remainingBytesStart = i + 1;
                 * if (remainingBytesStart < bufferLen) {
                 * System.arraycopy(byteArray, remainingBytesStart, byteArray, 0, bufferLen - remainingBytesStart);
                 * nameIndexStart = 0;
                 * } else {
                 * nameIndexStart = 0;
                 * }
                 */

            }

            if (ptr > 0) {
                handleBuffer(ptr, buffer, partitioner);
            }

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        showResultsAndWait(partitioner);

    }

    static void showResultsAndWait(Partitioner partitioner) {

        CountDownLatch c = new CountDownLatch(1);
        partitioner.scheduler.execute(() -> {
            while (!partitioner.allTasksCompleted()) {
            }
            SortedMap<Station, MeasurementAggregator> result = partitioner.getResult();

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

    private static void handleBuffer(int toProcessLen, byte[][] buffer, Partitioner partitioner) {
        // String[] toProcess = new String[toProcessLen];
        byte[][] toProcess = new byte[toProcessLen][];
        System.arraycopy(buffer, 0, toProcess, 0, toProcessLen);

        partitioner.scheduler.execute(() -> {
            processBatch(partitioner, toProcess, toProcessLen);
        });
    }

    private static void processBatch(Partitioner partitioner, byte[][] toProcess, int len) {

        for (int i = 0; i < len; i++) {
            partitioner.add(toProcess[i]);
        }
        partitioner.processSync();

    }

    private static Measurement getMeasurement(byte[] line) {
        int idx = lastIndexOf(line, ';');

        long temperature = parseToLongIgnoringDecimalPoint(line, idx + 1);

        // String substring = line.substring(0, idx);

        Measurement measurement = new Measurement(new Station(line, idx), temperature);
        return measurement;
    }

    private static int lastIndexOf(byte[] line, char findMe) {
        // we know that from the end of the line we have only
        // single byte chars
        for (int i = line.length - 1; i >= 0; i--) {
            if (line[i] == (byte) findMe) {
                return i;
            }
        }
        return -1;
    }

}
