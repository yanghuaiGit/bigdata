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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static compile.Word.getKeyWords;

public class Lexer {


    private char[] chars;
    private int index;
    private int length;

    private int line;

    private Word word;
    private String lexeme;

    private Map<String, Token> keyWords;


    public Lexer(String sql) {

        chars = sql.toCharArray();
        length = chars.length;
        index = 0;
        keyWords = new HashMap<>();
        for (Token keyWord : getKeyWords()) {
            keyWords.put(keyWord.getValue(), keyWord);
        }
    }

    private char getNextChar() {
        if (index == length) {
            return ' ';
        }
        char aChar = chars[index];
        index++;
        return aChar;
    }


    private char getEarNextChar() {
        if (index == length) {
            return ' ';
        }
        char aChar = chars[index];
        return aChar;
    }


    //有限状态机
    public Token scan() {
        lexeme = null;
        char nextChar = ' ';

        for (int i = 0; i < length; i++) {
            nextChar = getNextChar();
            if (nextChar == ' ' || nextChar == '\t') {
                continue;
            } else if (nextChar == '\n') {
                line++;
            } else {
                break;
            }
        }

        switch (nextChar) {
            case '{':
                lexeme = "{";
                return Token.LEFT_BRACE;
            case '}':
                lexeme = "}";
                return Token.RIGHT_BRACE;
            case '+':
                lexeme = "+";
                return Token.PLUS;
            case '-':
                lexeme = "-";
                return Token.MINUS;
            case '&': {
                if (getEarNextChar() == '&') {
                    getNextChar();
                    word = new Word(Token.AND, "&&");
                    lexeme = "&&";
                    return word.getToken();
                } else {
                    lexeme = "&";
                    return Token.AND_OPERATOR;
                }
            }
            case '|': {
                if (getEarNextChar() == '|') {
                    getNextChar();
                    word = new Word(Token.OR, "||");
                    lexeme = "||";
                    return word.getToken();
                } else {
                    lexeme = "|";
                    return Token.OR_OPERATOR;
                }
            }
            case '=': {
                if (getEarNextChar() == '=') {
                    getNextChar();
                    word = new Word(Token.EQ, "==");
                    lexeme = "==";
                    return word.getToken();
                } else {
                    lexeme = "=";
                    return Token.ASSIGN_OPERATOR;
                }
            }

            case '!': {
                if (getEarNextChar() == '=') {
                    getNextChar();
                    word = new Word(Token.NE, "!=");
                    lexeme = "!=";
                    return word.getToken();
                } else {
                    lexeme = "!";
                    return Token.NEGATE_OPERATOR;
                }
            }

            case '<': {
                if (getEarNextChar() == '=') {
                    getNextChar();
                    word = new Word(Token.LE, "<=");
                    lexeme = "<=";
                    return word.getToken();
                } else {
                    lexeme = "<";
                    return Token.LESS_OPERATOR;
                }
            }

            case '>': {
                if (getEarNextChar() == '=') {
                    getNextChar();
                    word = new Word(Token.GE, ">=");
                    lexeme = ">=";
                    return word.getToken();
                } else {
                    lexeme = ">";
                    return Token.GREATER_OPERATOR;
                }
            }
        }

        //读入的字符串是否是数字
        char tempChar = nextChar;
        if (Character.isDigit(tempChar)) {
            List<Character> charsList = new ArrayList();
            while (Character.isDigit(tempChar)) {
                charsList.add(tempChar);
                if (Character.isDigit(getEarNextChar())) {
                    tempChar = getNextChar();
                } else {
                    break;
                }
            }

            if (getEarNextChar() != '.') {
                //是一个整形数字
                StringBuilder stringBuilder = new StringBuilder();
                charsList.forEach(stringBuilder::append);
                lexeme = new BigDecimal(stringBuilder.toString()).toPlainString();
                return Token.NUM;
            }

            charsList.add(getNextChar());//把.去掉
            tempChar = getNextChar();
            while (Character.isDigit(tempChar)) {
                charsList.add(tempChar);
                if (Character.isDigit(getEarNextChar())) {
                    tempChar = getNextChar();
                } else {
                    break;
                }
            }
            StringBuilder stringBuilder = new StringBuilder();
            charsList.forEach(stringBuilder::append);
            lexeme = new BigDecimal(stringBuilder.toString()).toPlainString();
            return Token.REAL;
        }
        //读取变量字符串 注意关键字
        if (Character.isLetter(tempChar)) {
            List<Character> zifuList = new ArrayList();
            while (Character.isLetter(tempChar)) {
                zifuList.add(tempChar);
                if (Character.isLetter(getEarNextChar())) {
                    tempChar = getNextChar();
                } else {
                    break;
                }
            }
            StringBuilder stringBuilder = new StringBuilder();
            zifuList.forEach(stringBuilder::append);


            boolean b = keyWords.containsKey(stringBuilder.toString());
            if (b) {
                lexeme = stringBuilder.toString();
                return keyWords.get(stringBuilder.toString());
            } else {
                lexeme = stringBuilder.toString();
                return Token.ID;
            }
        }


        return Token.EOF;
    }

    public String getLexeme() {
        return lexeme;
    }
}
