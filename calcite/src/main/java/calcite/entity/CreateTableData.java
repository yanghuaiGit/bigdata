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

package calcite.entity;

import java.util.List;

public class CreateTableData extends Ddldata {
    private final String schema;
    private final String table;
    private final List<ColumnData> columnDataList;

    public CreateTableData(String schema, String table, List<ColumnData> columnDataList, EventType type, String sql) {
        super(type,sql);
        this.schema = schema;
        this.table = table;
        this.columnDataList = columnDataList;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public List<ColumnData> getColumnDataList() {
        return columnDataList;
    }
}
