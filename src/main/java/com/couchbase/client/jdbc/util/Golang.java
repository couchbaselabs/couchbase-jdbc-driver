/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.jdbc.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper methods for Go-style duration strings used by the Couchbase server API.
 * Vendored from com.couchbase.client.core.util.Golang (core-io), replacing
 * InvalidArgumentException with IllegalArgumentException to avoid a core-io dependency.
 */
public class Golang {

    private static final Pattern durationComponentPattern = Pattern.compile("(?:([\\d.]+)([^\\d.]*))");
    private static final Pattern durationPattern = Pattern.compile(durationComponentPattern.pattern() + "+");

    private Golang() {
    }

    public static String encodeDurationToMs(final Duration duration) {
        return duration.toMillis() + "ms";
    }

    /**
     * Parses a Go duration string (e.g. "300ms", "1.5h", "2h45m").
     * Valid units: ns, us (or µs), ms, s, m, h.
     *
     * @throws IllegalArgumentException if the input is not a valid Go duration string.
     */
    public static Duration parseDuration(final String duration) {
        final boolean negative = duration.startsWith("-");
        final String abs = negative || duration.startsWith("+") ? duration.substring(1) : duration;

        if (abs.equals("0")) {
            return Duration.ZERO;
        }

        try {
            final Matcher validator = durationPattern.matcher(abs);
            if (!validator.matches()) {
                throw new IllegalArgumentException("Invalid duration.");
            }

            long resultNanos = 0;

            final Matcher m = durationComponentPattern.matcher(abs);
            while (m.find()) {
                final BigDecimal number = new BigDecimal(m.group(1));
                final TimeUnit timeUnit = parseTimeUnit(m.group(2));
                resultNanos = Math.addExact(resultNanos, toNanosExact(number, timeUnit));
            }

            return Duration.ofNanos(negative ? -resultNanos : resultNanos);

        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Duration \"" + duration + "\" is too long. Maximum duration is 2^63-1 nanoseconds.", e);

        } catch (IllegalArgumentException e) {
            throw e;

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse duration \"" + duration + "\": " + e.getMessage() +
                " ; valid units: ns, us, ms, s, m, h", e);
        }
    }

    private static long toNanosExact(BigDecimal number, TimeUnit timeUnit) {
        return number.multiply(BigDecimal.valueOf(timeUnit.toNanos(1)))
            .setScale(0, RoundingMode.DOWN)
            .longValueExact();
    }

    private static TimeUnit parseTimeUnit(String unit) {
        switch (unit) {
            case "":
                throw new IllegalArgumentException("Missing time unit.");
            case "h":
                return TimeUnit.HOURS;
            case "m":
                return TimeUnit.MINUTES;
            case "s":
                return TimeUnit.SECONDS;
            case "ms":
                return TimeUnit.MILLISECONDS;
            case "us":
            case "µs":
            case "μs":
                return TimeUnit.MICROSECONDS;
            case "ns":
                return TimeUnit.NANOSECONDS;
            default:
                throw new IllegalArgumentException("Unknown time unit \"" + unit + "\".");
        }
    }
}
