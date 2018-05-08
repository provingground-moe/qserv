parser grammar QSMySqlParser;

import MySqlParser;

options { tokenVocab=QSMySqlLexer; }

 
// same as MySqlParser except:
// * adds (val, min, max) keywords to betweenPredicate
predicate
   : predicate NOT? IN '(' (selectStatement | expressions) ')'     #inPredicate
   | predicate IS nullNotnull                                      #isNullPredicate
   | left=predicate comparisonOperator right=predicate             #binaryComparasionPredicate
   | predicate comparisonOperator 
     quantifier=(ALL | ANY | SOME) '(' selectStatement ')'         #subqueryComparasionPredicate
   | val=predicate NOT? BETWEEN min=predicate AND max=predicate    #betweenPredicate
   | predicate SOUNDS LIKE predicate                               #soundsLikePredicate
   | predicate NOT? LIKE predicate (ESCAPE STRING_LITERAL)?        #likePredicate
   | predicate NOT? regex=(REGEXP | RLIKE) predicate               #regexpPredicate
   | (LOCAL_ID VAR_ASSIGN)? expressionAtom                         #expressionAtomPredicate
   ;
 
 
 decimalLiteral
    : MINUS? DECIMAL_LITERAL | ZERO_DECIMAL | ONE_DECIMAL | TWO_DECIMAL
    ;
