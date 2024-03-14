package org.apache.flink.streaming.examples.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class DataGenWordCount {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setString("state.backend.type", "rocksdb");
        config.setString("io.tmp.dirs", "/tmp/io");
        config.setInteger("execution.checkpointing.tolerable-failed-checkpoints", Integer.MAX_VALUE);
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/flink/checkpoints");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        config.set(CheckpointingOptions.LOCAL_RECOVERY, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(20000);

        GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;

        DataGeneratorSource<String> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1000),
                        Types.STRING);

        DataStreamSource<String> streamSource =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");
        streamSource.keyBy(d -> d)
                        .flatMap(new WordCountFlatMapper())
                                .addSink(new DummySink());

        env.execute("Data Generator Word Count");
    }

    public static class WordCountFlatMapper extends RichFlatMapFunction<String, Long> {

        private transient ValueState<Long> wordCounter;

        public WordCountFlatMapper() {
        }

        @Override
        public void flatMap(String in, Collector<Long> out) throws IOException {
            Long currentValue = wordCounter.value();

            if (currentValue != null) {
                wordCounter.update(currentValue + 1);
            } else {
                wordCounter.update(1L);
            }

            out.collect(currentValue);
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>(
                            "wc",
                            TypeInformation.of(new TypeHint<Long>(){}));
            wordCounter = getRuntimeContext().getState(descriptor);
        }
    }

    private static class DummySink extends RichSinkFunction<Long> {

        @Override
        public void invoke(Long value, Context context) throws Exception {
            // Do nothing.
        }
    }
}
