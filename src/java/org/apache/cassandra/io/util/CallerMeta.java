/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io.util;

import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.OperationType;

public final class CallerMeta {
    public final OperationType opType;
    public final UUID compactionId;
    public final String src;

    private final String logAux;

    private CallerMeta(String source, OperationType o, UUID id) {
        src = source;
        opType = o;
        compactionId = id;
        logAux = makeAux();
    }

    private String makeAux() {
        StringBuilder sb = new StringBuilder();
        if (src != null) {
            sb.append("src=");
            sb.append(src);
            sb.append(',');
        }
        if (opType != null) {
            sb.append("op=");
            sb.append(opType.toString());
            sb.append(',');
        }
        if (compactionId != null) {
            sb.append("cpctid=");
            sb.append(compactionId.toString());
            sb.append(',');
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length()-1);
        }
        return sb.toString();
    }

    public CallerMeta withSource(String source)
    {
        return of(source, opType, compactionId);
    }

    public static String logAux(Optional<CallerMeta> meta) {
        if (meta.isPresent()) {
            return meta.get().logAux;
        }
        return "";
    }

    public static CallerMeta of(String src, OperationType opType, UUID compactionId) {
        if (DatabaseDescriptor.logWriteOps()) {
            return new CallerMeta(src, opType, compactionId);
        }
        return null;
    }

    public static CallerMeta from(Optional<CallerMeta> base, String src) {
        if (!base.isPresent()) {
            return null;
        }
        return of(src, base.get().opType, base.get().compactionId);
    }
}
