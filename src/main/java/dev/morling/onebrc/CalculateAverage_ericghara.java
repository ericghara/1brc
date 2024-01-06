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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

class MeasurementAggregator {
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private long sum = 0;
    private long count = 0;

    void add(int measurement) {
        sum += measurement;
        count++;
        if (min > measurement) {
            min = measurement;
        }
        if (max < measurement) {
            max = measurement;
        }
    }
    void merge(MeasurementAggregator other) {
        this.sum += other.sum;
        this.count += other.count;
        if (this.min > other.min) {
            this.min = other.min;
        }
        if (this.max < other.max) {
            this.max = other.max;
        }
    }
}

class WorkerContext {

    private static final char END_DELIM = '\n';

    private final MappedByteBuffer byteBuffer;
    private final int workerId;

    WorkerContext(MappedByteBuffer byteBuffer, int workerId) {
        this.byteBuffer = byteBuffer;
        this.workerId = workerId;
    }

    public void align() {
        // no alignment required for beginning of file
        if (this.workerId > 0) {
            for (int i = 0; i < byteBuffer.limit(); i++) {
                // if start is at an end delim need to read to next delim b/c next segment will search forward
                if (byteBuffer.get(i) == END_DELIM && i > 0) {
                    byteBuffer.position(i+1);
                    break;
                }
            }
        }
        byteBuffer.mark();
        // align end
        int segEnd = byteBuffer.limit()-1;
        byteBuffer.limit(byteBuffer.capacity());
        for (int i = segEnd; i < byteBuffer.limit(); i++) {
            // pass delim if it is at very end of segment because next seg would not be able to know it begins aligned
            if (byteBuffer.get(i) == END_DELIM && i > segEnd) {
                byteBuffer.limit(i+1);
                break;
            }
        }
        byteBuffer.reset();
    }

    public void parse() {

    }

    public WorkerContext merge(WorkerContext other) {
        return this;
    }

    public void print(PrintStream stream) {

    }
  }

public class CalculateAverage_ericghara {

    private static final String FILE = "./measurements.txt";
    private static final int THREADS = 8;
    // overlap between file segments, allows segments to align to new line
    private static final int OVERLAP_B = 256;

    // line end char
    private static final char END_DELIM = '\n';

    private static PrintStream printStream = new PrintStream(System.out, false);

    public static List<MappedByteBuffer> mapFile(String filename) {
        // attribution: spullara mmap file
        List<MappedByteBuffer> byteBuffers = new ArrayList<>(THREADS);
        try (var fileChannel = (FileChannel) Files.newByteChannel( Path.of( filename), StandardOpenOption.READ)) {
            if (fileChannel.size() <= OVERLAP_B) {
                byteBuffers.add(fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size()));
            }
            else {
                int step = (int) fileChannel.size()/THREADS;
                int remainder = (int) fileChannel.size()%THREADS;
                long pos = 0;
                while (pos < fileChannel.size()) {
                    int segLength = step + (remainder > 0 ? 1 : 0);
                    var byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, pos,
                                                    Math.min(segLength+OVERLAP_B, fileChannel.size()-pos));
                    byteBuffer.limit(segLength);  // to communicate begin of overlap to align fn
                    byteBuffers.add(byteBuffer);
                    remainder--;
                    pos += segLength;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to open file.", e);
        }
        return byteBuffers;
    }

    public static void main(String[] args) throws IOException {
        String filename = args.length == 0 ? FILE : args[0];
        List<MappedByteBuffer> byteBuffers = mapFile(filename);
        ConcurrentLinkedQueue<Supplier<WorkerContext>> ctxs = new ConcurrentLinkedQueue<>();
         try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
             for (int i = 0; i < byteBuffers.size(); i++) {
                 final int workerId = i;
                Supplier<WorkerContext> ctxSup = scope.fork( () -> {
                    var ctx = new WorkerContext(byteBuffers.get(workerId), workerId);
                    ctx.align();
                    ctx.parse();
                    return ctx;
                });
                ctxs.offer(ctxSup);
             }
             scope.join().throwIfFailed();
             while (ctxs.size() > 1) {
                 // don't merge last if odd number, remains at head to be merged in next round
                 for (int i = ctxs.size(); i > 1; i -= 2) {
                     final WorkerContext a = ctxs.poll().get();
                     final WorkerContext b = ctxs.poll().get();
                     Supplier<WorkerContext> ctxSup = scope.fork(() -> a.merge(b));
                     ctxs.offer(ctxSup);
                 }
                 scope.join().throwIfFailed();
             }
         } catch (InterruptedException | ExecutionException e) {
             throw new RuntimeException(e);
         }
         if (!ctxs.isEmpty()) {
             ctxs.peek().get().print(printStream);
             printStream.flush();
         }
    }
}
