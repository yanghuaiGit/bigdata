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

package table.connector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;

public class kakfaSource {
    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.connect(new Kafka()
//                        .version("universal")
                        .version("0.11")
                        .topic("clicks")
                        .property("group.id", "click-group")
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kudu1:9092")
                        .startFromEarliest()
                )
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT())
                )
                .withFormat(new Csv())
                .createTemporaryTable("sensor");


        //将连接器应用转换为表
        Table sensor = tableEnv.from("sensor");

        Table select = sensor.groupBy($("id"))
                .select($("id"), $("id").count().as("ct"));

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(select, Row.class);
        tuple2DataStream.print();

        env.execute();
    }
}
