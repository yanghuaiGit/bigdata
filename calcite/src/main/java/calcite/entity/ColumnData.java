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

import org.apache.calcite.plan.Strong;

public class ColumnData {
    /**字段名称*/
    private String name;
    /**字段类型*/
    private Type type;
    /**字段是否可以为空*/
    private boolean nullable;
    /**是否是主键*/
    private boolean isPrimary;
    /**字段默认值*/
    private String defaultValue;
    /**字段描述*/
    private String comment;
    /**字段长度*/
    protected Integer length;

    /**小数点长度*/
    protected Integer digital;

    /** 字段精度 */
    protected Integer precision;

    public ColumnData(String name, Type type, boolean nullable, boolean isPrimary, String defaultValue, String comment, Integer length, Integer digital, Integer precision) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.isPrimary = isPrimary;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.length = length;
        this.digital = digital;
        this.precision = precision;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getComment() {
        return comment;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getDigital() {
        return digital;
    }

    public Integer getPrecision() {
        return precision;
    }
}
