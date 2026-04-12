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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

/**
 * rb_build(value INT) -> BYTES
 *
 * <p>Scalar function that creates a serialized RoaringBitmap containing a single
 * 32-bit integer value. This is the row-level counterpart of {@link RbBuildAggFunction}:
 * use this when each row should produce its own single-element bitmap (e.g. for
 * storage-layer merge engines that handle aggregation at write time).
 *
 * <p>Usage in Flink SQL:
 * <pre>{@code
 * CREATE TEMPORARY FUNCTION rb_build
 *     AS 'org.apache.flink.udfs.bitmap.RbBuildFunction';
 *
 * INSERT INTO bitmap_table
 * SELECT dimension, rb_build(uid) AS bitmap, 1 AS cnt
 * FROM events;
 * }</pre>
 */
public class RbBuildFunction extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    @DataTypeHint("BYTES")
    @Nullable
    public byte[] eval(@Nullable Integer value) {
        if (value == null) {
            return null;
        }
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(value);
        bitmap.runOptimize();
        return BitmapUtils.toBytes(bitmap);
    }
}
