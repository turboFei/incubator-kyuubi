/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar KyuubiEbaySparkSQL;

@members {
   /**
    * Verify whether current token is a valid decimal token (which contains dot).
    * Returns true if the character that follows the token is not a digit or letter or underscore.
    *
    * For example:
    * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
    * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
    * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
    * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
    * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
    * which is not a digit or letter or underscore.
    */
   public boolean isValidDecimal() {
     int nextChar = _input.LA(1);
     if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
       nextChar == '_') {
       return false;
     } else {
       return true;
     }
   }
 }

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

statement
    : UPLOAD DATA INPATH path=STRING OVERWRITE? INTO TABLE
        multipartIdentifier partitionSpec? optionSpec?              #uploadData
    | MOVE DATA INPATH path=STRING OVERWRITE? INTO
        destDir=STRING (destFileName=STRING)?                       #moveData
    | KYUUBI DESCRIBE PATH EXTENDED? multipartIdentifier            #kyuubiDescribePath
    | KYUUBI CREATE PATH path=STRING                                #kyuubiCreatePath
    | KYUUBI DELETE PATH path=STRING RECURSIVE?                     #kyuubiDeletePath
    | KYUUBI RENAME PATH path=STRING OVERWRITE? TO dest=STRING      #kyuubiRenamePath
    | KYUUBI LIST PATH path=STRING limitSpec?                       #kyuubiListPath
    | .*?                                                           #passThrough
    ;

limitSpec
    : LIMIT limit=number
    ;

partitionSpec
    : PARTITION '(' partitionVal (',' partitionVal)* ')'
    ;

partitionVal
    : identifier (EQ constant)?
    ;

optionSpec
    : OPTION '(' optionVal (',' optionVal)* ')'
    ;

optionVal
    : identifier (EQ constant)?
    ;

query
    : '('? multipartIdentifier comparisonOperator constant ')'?
    ;

comparisonOperator
    : EQ | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ
    ;

constant
    : NULL                     #nullLiteral
    | identifier STRING        #typeConstructor
    | number                   #numericLiteral
    | booleanValue             #booleanLiteral
    | STRING+                  #stringLiteral
    ;

multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
    ;

booleanValue
    : TRUE | FALSE
    ;

number
    : MINUS? DECIMAL_VALUE             #decimalLiteral
    | MINUS? INTEGER_VALUE             #integerLiteral
    | MINUS? BIGINT_LITERAL            #bigIntLiteral
    | MINUS? SMALLINT_LITERAL          #smallIntLiteral
    | MINUS? TINYINT_LITERAL           #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL            #doubleLiteral
    | MINUS? BIGDECIMAL_LITERAL        #bigDecimalLiteral
    ;

identifier
    : strictIdentifier
    ;

strictIdentifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | nonReserved             #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

nonReserved
    : AND
    | CREATE
    | DATA
    | DELETE
    | DESCRIBE
    | EXTENDED
    | FALSE
    | INPATH
    | INTERVAL
    | INTO
    | KYUUBI
    | LIMIT
    | LIST
    | MOVE
    | OPTIMIZE
    | OPTION
    | OR
    | OVERWRITE
    | PARTITION
    | PATH
    | RECURSIVE
    | RENAME
    | TABLE
    | TIMESTAMP
    | TO
    | TRUE
    | UPLOAD
    ;

AND: 'AND';
CREATE: 'CREATE';
DATA: 'DATA';
DELETE: 'DELETE';
DESCRIBE: 'DESCRIBE';
EXTENDED: 'EXTENDED';
FALSE: 'FALSE';
INPATH: 'INPATH';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
KYUUBI: 'KYUUBI';
LIMIT: 'LIMIT';
LIST: 'LIST';
MOVE: 'MOVE';
NULL: 'NULL';
OPTIMIZE: 'OPTIMIZE';
OPTION: 'OPTION';
OR: 'OR';
OVERWRITE: 'OVERWRITE';
PARTITION: 'PARTITION';
PATH: 'PATH';
RECURSIVE: 'RECURSIVE';
RENAME: 'RENAME';
TABLE: 'TABLE';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRUE: 'TRUE';
UPLOAD: 'UPLOAD';

EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';

MINUS: '-';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS  : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
