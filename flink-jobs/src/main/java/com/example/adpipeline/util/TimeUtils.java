package com.example.adpipeline.util;

import java.time.Duration;

public final class TimeUtils {
    public static final Duration WATERMARK_DELAY = Duration.ofMinutes(10);

    private TimeUtils() {
    }
}
