package com.example.adpipeline.util;

import java.time.Duration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class FlinkJobConfig {
    private static final long DEFAULT_CHECKPOINT_INTERVAL_MS = Duration.ofMinutes(1).toMillis();
    private static final long DEFAULT_CHECKPOINT_MIN_PAUSE_MS = Duration.ofSeconds(10).toMillis();
    private static final long DEFAULT_CHECKPOINT_TIMEOUT_MS = Duration.ofMinutes(5).toMillis();
    private static final String DEFAULT_CHECKPOINT_DIR = "file:///opt/flink/checkpoints";
    private static final int DEFAULT_RESTART_ATTEMPTS = 3;
    private static final long DEFAULT_RESTART_DELAY_MS = Duration.ofSeconds(10).toMillis();
    private static final int DEFAULT_PARALLELISM = 1;

    private FlinkJobConfig() {
    }

    public static void apply(StreamExecutionEnvironment env, ParameterTool params) {
        env.setParallelism(params.getInt("parallelism", DEFAULT_PARALLELISM));
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(
            params.getLong("checkpoint-interval-ms", DEFAULT_CHECKPOINT_INTERVAL_MS),
            CheckpointingMode.EXACTLY_ONCE
        );

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(params.get("checkpoint-dir", DEFAULT_CHECKPOINT_DIR));
        checkpointConfig.setMinPauseBetweenCheckpoints(
            params.getLong("checkpoint-min-pause-ms", DEFAULT_CHECKPOINT_MIN_PAUSE_MS)
        );
        checkpointConfig.setCheckpointTimeout(params.getLong("checkpoint-timeout-ms", DEFAULT_CHECKPOINT_TIMEOUT_MS));
        checkpointConfig.setMaxConcurrentCheckpoints(params.getInt("checkpoint-max-concurrent", 1));
        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(
                params.getInt("restart-attempts", DEFAULT_RESTART_ATTEMPTS),
                params.getLong("restart-delay-ms", DEFAULT_RESTART_DELAY_MS)
            )
        );
    }
}
