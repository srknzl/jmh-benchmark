/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.hazelcast.query.impl.getters;

import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Benchmark;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

class IdentifiedDataWithLongCompactPojo {
    public Integer[] numbers;
    public Long value;

    public IdentifiedDataWithLongCompactPojo() {
    }

    public IdentifiedDataWithLongCompactPojo(Integer[] numbers, Long value) {
        this.numbers = numbers;
        this.value = value;
    }
}

class IdentifiedDataWithLongCompactSerializer implements CompactSerializer<IdentifiedDataWithLongCompactPojo> {
    @NotNull
    @Override
    public IdentifiedDataWithLongCompactPojo read(@NotNull CompactReader compactReader) {
        IdentifiedDataWithLongCompactPojo pojo = new IdentifiedDataWithLongCompactPojo();
        pojo.value = compactReader.readInt64("value");
        pojo.numbers = new Integer[compactReader.readInt32("numbers-size")];
        for (int i = 0; i < pojo.numbers.length; i++) {
            if (compactReader.readBoolean("numbers-present-" + i)) {
                pojo.numbers[i] = compactReader.readInt32("numbers-" + i);
            }
        }
        return pojo;
    }

    @Override
    public void write(@NotNull CompactWriter compactWriter, @NotNull IdentifiedDataWithLongCompactPojo pojo) {
        compactWriter.writeInt64("value", pojo.value);
        compactWriter.writeInt32("numbers-size", pojo.numbers.length);
        for (int i = 0; i < pojo.numbers.length; i++) {
            Integer number = pojo.numbers[i];
            if (number != null) {
                compactWriter.writeBoolean("numbers-present-" + i, true);
                compactWriter.writeInt32("numbers-" + i, number);
            } else {
                compactWriter.writeBoolean("numbers-present-" + i, false);
            }
        }
    }
}


@State(Scope.Benchmark)
public class MyBenchmark {
    private final GenericRecordQueryReader reader;
    HazelcastInstance instance;
    CompactGetter compactGetter;
    Data data;

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    @Warmup(iterations = 2, time = 3)
    public void compact(Blackhole blackhole) throws Exception {
        for (int i = 0; i < 1000; i++) {
            blackhole.consume(reader.read("value"));
        }
    }

    @TearDown
    public void destroy() {
        instance.shutdown();
    }

    public MyBenchmark() {
        instance = Hazelcast.newHazelcastInstance(new Config().setSerializationConfig(
                new SerializationConfig()
                        .setCompactSerializationConfig(
                                new CompactSerializationConfig()
                                        .setEnabled(true)
                                        .register(IdentifiedDataWithLongCompactPojo.class, "identifiedDataWithLongCompactPojo", new IdentifiedDataWithLongCompactSerializer()))
        ));
        HazelcastInstanceProxy hazelcastInstanceImpl = (HazelcastInstanceProxy) instance;
        InternalSerializationService serializationService = hazelcastInstanceImpl.getSerializationService();
        compactGetter = new CompactGetter(serializationService);

        IdentifiedDataWithLongCompactPojo pojo = new IdentifiedDataWithLongCompactPojo(new Integer[20], 0L);
        data = serializationService.toData(pojo);
        try {
            InternalGenericRecord internalGenericRecord = serializationService.readAsInternalGenericRecord(data);
            reader = new GenericRecordQueryReader(internalGenericRecord);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

