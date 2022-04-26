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

package compile;

public enum Token {
    AND("&&","AND"),
    OR("||","OR"),
    EQ("==","EQ"),
    LE("<=","LE"),
    GE(">=","GE"),
    NE("!=","NE"),
    PLUS("+","PLUS"),
    MINUS("-","MINUS"),
    IF("if","IF"),
    ELSE("else","ELSE"),
    ID("identifer","ID"),
    LEFT_BRACE("{","LEFT_BRACE"),
    RIGHT_BRACE("}","RIGHT_BRACE"),

    NUM("NUMBER","NUM"),//整数
    REAL("REAL","REAL"),//实数 浮点数

    AND_OPERATOR("&","AND_OPERATOR"),
    OR_OPERATOR("|","OR_OPERATOR"),
    ASSIGN_OPERATOR("=","ASSIGN_OPERATOR"),
    NEGATE_OPERATOR("!","NEGATE_OPERATOR"),
    LESS_OPERATOR("<=","LESS_OPERATOR"),
    GREATER_OPERATOR(">=","GREATER_OPERATOR"),

    EOF("","EOF" )
   ;
    private String value;
    private String tag;

    public String getValue() {
        return value;
    }

    public String getTag() {
        return tag;
    }

    Token(String value, String tag) {
        this.value = value;
        this.tag = tag;
    }
}
