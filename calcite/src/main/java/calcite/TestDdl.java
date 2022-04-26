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

package calcite;

import calcite.entity.DdlConventManager;
import calcite.entity.DdlRowData;
import calcite.entity.LogConventExceptionProcessHandler;
import calcite.entity.MysqlCalciteSqlDdlConvent;
import org.junit.Test;

import java.util.Properties;

public class TestDdl {
    @Test
    public void test() {

        String schema = "dujie";
        String table = "test";
        String sql = "create table test (column_1 int null,column_2 int not null)";

        DdlRowData rowData = getRowData(schema, table, sql);

        MysqlCalciteSqlDdlConvent source = new MysqlCalciteSqlDdlConvent(getCustomProperties());
        MysqlCalciteSqlDdlConvent sink = new MysqlCalciteSqlDdlConvent(getCustomProperties());
        DdlConventManager ddlConventManager = new DdlConventManager(source, sink, new LogConventExceptionProcessHandler());

        String conventedSql = ddlConventManager.conventDdl(rowData);
        System.out.println(conventedSql);
    }

    private Properties getCustomProperties() {
        Properties properties = new Properties();
        properties.put("ENGINE", "InnoDB");
        properties.put("DEFAULT CHARSET", "utf8mb4");
        properties.put("COLLATE", "utf8mb4_0900_ai_ci");
        return properties;
    }

    private DdlRowData getRowData(String schema, String table, String sql) {
        String[] headers = new String[3];
        headers[0] = "schema";
        headers[1] = "table";
        headers[2] = "sql";
        DdlRowData ddlRowData = new DdlRowData(headers);
        ddlRowData.setDdlInfo(0, schema);
        ddlRowData.setDdlInfo(1, table);
        ddlRowData.setDdlInfo(2, sql);
        return ddlRowData;
    }



    // Sql语句
//        String sql = "CREATE TABLE `test120` (\n" +
//                "  `column_1` int NOT NULL,\n" +
//                "  `column_2` varchar(255) DEFAULT '123'," +
//                "  `tin` tinyint DEFAULT NULL\n" +
//                ")";
}
