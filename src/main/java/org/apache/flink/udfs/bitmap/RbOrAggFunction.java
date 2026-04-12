/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.udfs.bitmap;

import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

/**
 * rb_or_agg(bitmap BYTES) -> BYTES
 *
 * <p>Aggregates multiple serialized RoaringBitmaps using OR (union). Used in
 * roll-up aggregation scenarios where per-group bitmaps (e.g., per minute)
 * need to be merged into a coarser granularity (e.g., per hour).
 *
 * <p>Usage in Flink SQL (after registering the JAR):
 * <pre>{@code
 * CREATE TEMPORARY FUNCTION rb_or_agg
 *     AS 'org.apache.flink.udfs.bitmap.RbOrAggFunction';
 *
 * SELECT
 *     hour_start,
 *     rb_cardinality(rb_or_agg(minute_bitmap)) AS unique_visitors
 * FROM minute_stats
 * GROUP BY hour_start;
 * }</pre>
 */
public class RbOrAggFunction extends AbstractRbAggFunction {

    /**
     * OR-s the input bitmap into the accumulator.
     *
     * @param acc         the running bitmap accumulator
     * @param bitmapBytes serialized RoaringBitmap bytes to union in;
     *                    null and empty arrays are ignored
     */
    public void accumulate(RoaringBitmap acc, @Nullable byte[] bitmapBytes) {
        if (bitmapBytes == null || bitmapBytes.length == 0) {
            return;
        }
        RoaringBitmap input = BitmapUtils.fromBytes(bitmapBytes);
        acc.or(input);
    }

    /**
     * Retract is not supported for bitmap OR (union is not reversible).
     * This method exists to satisfy Flink's streaming GROUP BY planner,
     * but will throw if actually invoked on a retract stream.
     */
    public void retract(RoaringBitmap acc, @Nullable byte[] bitmapBytes) {
        throw new UnsupportedOperationException(
                "rb_or_agg does not support retraction. "
                        + "Use it only on append-only streams.");
    }
}
