// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.doris.flink.source;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.source.assigners.DorisSplitAssigner;
import org.apache.doris.flink.source.assigners.SimpleSplitAssigner;
import org.apache.doris.flink.source.enumerator.DorisSourceEnumerator;
import org.apache.doris.flink.source.enumerator.PendingSplitsCheckpoint;
import org.apache.doris.flink.source.enumerator.PendingSplitsCheckpointSerializer;
import org.apache.doris.flink.source.reader.DorisRecordEmitter;
import org.apache.doris.flink.source.reader.DorisSourceReader;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisSourceSplitSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * DorisSource based on FLIP-27 which is a BOUNDED stream.
 **/
public class DorisSource<OUT> implements Source<OUT, DorisSourceSplit, PendingSplitsCheckpoint>,
        ResultTypeQueryable<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSource.class);

    private final DorisOptions options;
    private final DorisReadOptions readOptions;

    // Boundedness
    private final Boundedness boundedness;
    private final DorisDeserializationSchema<OUT> deserializer;

    public DorisSource(DorisOptions options,
                       DorisReadOptions readOptions,
                       Boundedness boundedness,
                       DorisDeserializationSchema<OUT> deserializer) {
        this.options = options;
        this.readOptions = readOptions;
        this.boundedness = boundedness;
        this.deserializer = deserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    /**
     * 创建SourceReader
     * @param readerContext
     * @return
     * @throws Exception
     */
    @Override
    public SourceReader<OUT, DorisSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new DorisSourceReader<>(
                options,
                readOptions,
                // doris数据反序列化与输出器
                new DorisRecordEmitter<>(deserializer),
                readerContext,
                readerContext.getConfiguration()
        );
    }

    /**
     * 创建数据枚举器，负责读取split并且分配给特定的subtask
     * @param context
     * @return
     * @throws Exception
     */
    @Override
    public SplitEnumerator<DorisSourceSplit, PendingSplitsCheckpoint> createEnumerator(SplitEnumeratorContext<DorisSourceSplit> context) throws Exception {
        List<DorisSourceSplit> dorisSourceSplits = new ArrayList<>();
        // 根据配置拉取doris的partition配置 主要包含db、table、tablet、beAddress等
        List<PartitionDefinition> partitions = RestService.findPartitions(options, readOptions, LOG);
        // 构建DorisSourceSplit集合
        partitions.forEach(m -> dorisSourceSplits.add(new DorisSourceSplit(m)));
        // 将split分配
        DorisSplitAssigner splitAssigner = new SimpleSplitAssigner(dorisSourceSplits);

        // 构建数据源枚举器
        return new DorisSourceEnumerator(context, splitAssigner);
    }

    /**
     * 从ck中获取split，然后放入枚举器中
     * @param context
     * @param checkpoint
     * @return
     * @throws Exception
     */
    @Override
    public SplitEnumerator<DorisSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<DorisSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) throws Exception {
        Collection<DorisSourceSplit> splits = checkpoint.getSplits();
        DorisSplitAssigner splitAssigner = new SimpleSplitAssigner(splits);
        return new DorisSourceEnumerator(context, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<DorisSourceSplit> getSplitSerializer() {
        return DorisSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializer.getProducedType();
    }
}
