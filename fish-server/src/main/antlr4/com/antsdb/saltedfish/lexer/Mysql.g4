/* 
 * sql parser my mysql accent
 * 
 * there are four kinds of basic tokens
 * 
 * name
 * 
 * identifier
 * 
 * value
 * 
 * keyword
 */

grammar Mysql;

@members {
    public Object lastStatement;
}

parse
 : script
 ;

script
 : sql_stmt (';' sql_stmt?)* EOF
 ; 

sql_stmt
 : (
   alter_table_stmt
 | alter_user_stmt
 | begin_stmt
 | change_master_stmt
 | commit_stmt
 | create_database_stmt
 | create_index_stmt
 | create_table_stmt
 | create_table_like_stmt
 | create_user_stmt
 | delete_stmt
 | drop_database_stmt
 | drop_index_stmt
 | drop_table_stmt
 | drop_user_stmt
 | explain_stmt
 | insert_stmt
 | kill_stmt
 | load_data_infile_stmt
 | lock_table_stmt
 | rollback_stmt
 | select_stmt
 | set_stmt
 | show_charset
 | show_collation
 | show_columns_stmt
 | show_create_table_stmt
 | show_databases
 | show_engines
 | show_function_stmt
 | show_grants
 | show_index_stmt
 | show_master_status
 | show_privileges
 | show_procedure
 | show_processlist
 | show_status
 | show_table_status_stmt
 | show_tables_stmt
 | show_triggers_stmt
 | show_variable_stmt
 | show_warnings_stmt
 | truncate_table_stmt
 | unlock_table_stmt
 | update_stmt
 | use_stmt
 | start_transaction
 | start_slave
 | stop_slave
 | delete_stmt__
 ) { this.lastStatement = _localctx; }
 ;

alter_user_stmt
 : K_ALTER K_USER string_value K_IDENTIFIED K_BY string_value
 ;
 
begin_stmt
 : K_BEGIN
 ;
 
delete_stmt__
 : K___DELETE number_value K_WHERE K_KEY '=' STRING_LITERAL 
 ;
 
start_transaction
 : K_START K_TRANSACTION 
 ;

start_slave
 : K_START K_SLAVE 
 ;

stop_slave
 : K_STOP K_SLAVE 
 ;

alter_table_stmt
 : K_ALTER K_TABLE table_name_ alter_table_options (',' alter_table_options)*
 ;

alter_table_options
 : alter_table_add 
 | alter_table_add_constraint
 | alter_table_add_index 
 | alter_table_add_primary_key
 | alter_table_disable_keys
 | alter_table_enable_keys
 | alter_table_modify
 | alter_table_rename
 | alter_table_drop
 | alter_table_drop_index
 ;

alter_table_add
 : K_ADD K_COLUMN? column_def_set
 ;

column_def_set
 : column_def
 | column_def_list
 ;
 
column_def_list
 : '('  column_def (',' column_def)* ')'
 ;
 
alter_table_add_constraint
 : K_ADD (K_CONSTRAINT identifier)? (alter_table_add_constraint_fk | alter_table_add_constraint_pk)
 ;

alter_table_add_constraint_fk
 : K_FOREIGN K_KEY '(' child_columns ')' K_REFERENCES table_name_ '(' parent_columns ')' cascade_option*
 ;
 
alter_table_add_constraint_pk
 : K_PRIMARY K_KEY '(' columns ')' 
 ;
 
alter_table_add_index
 : K_ADD ((K_UNIQUE? K_INDEX) | K_FULLTEXT) index_name? '(' indexed_column_def ( ',' indexed_column_def )* ')'
 ;
 
alter_table_add_primary_key
 : K_ADD K_PRIMARY K_KEY '(' columns ')'
 ;
 
child_columns: columns;

parent_columns: columns;

columns: identifier (',' identifier)*;

alter_table_disable_keys: K_DISABLE K_KEYS;

alter_table_enable_keys: K_ENABLE K_KEYS;

alter_table_rename
 : (K_CHANGE | K_MODIFY) K_COLUMN? identifier? column_def
 ;

alter_table_modify_list
 : alter_table_modify  (',' alter_table_modify)*
 ;
 
alter_table_modify
 : (K_CHANGE | K_MODIFY) K_COLUMN? column_def
 ;
 
new_col_name
 : identifier
 ;

alter_table_drop_list
 : alter_table_drop (',' alter_table_drop)*
 ;
  
alter_table_drop
 : K_DROP K_COLUMN? identifier
 ;
 
alter_table_drop_index
 : K_DROP (K_INDEX | K_KEY) identifier
 ;
 
change_master_stmt
 : K_CHANGE K_MASTER K_TO (change_master_stmt_option (',' change_master_stmt_option)*) 
 ;

change_master_stmt_option
 : WORD '=' literal_value
 ;
  
commit_stmt
 : K_COMMIT
 ;
 
create_database_stmt
 : K_CREATE (K_DATABASE | K_SCHEMA) (K_IF K_NOT K_EXISTS)? identifier (K_DEFAULT? K_CHARACTER K_SET any_name)?  
 ;
 
create_index_stmt:
   K_CREATE ((K_UNIQUE? K_FULLTEXT?) | K_FULLTEXT) K_INDEX ( K_IF K_NOT K_EXISTS )?
   index_name K_ON table_name_ '(' indexed_column_def ( ',' indexed_column_def )* ')' index_type?
 ;

indexed_column_def
 : indexed_column ('(' signed_number ')')?
 ;
  
create_table_stmt 
 : K_CREATE K_TEMPORARY? K_TABLE (K_IF K_NOT K_EXISTS)? 
   table_name_ 
   ('(' create_defs  ','? ')')? 
   table_options
   select_or_values?
 ;

create_defs
 : create_def (',' create_def)*
 ;
 
create_def
 : column_def
 | primary_key_def
 | index_def
 | constraint_def
 ;
 
column_def
 : column_name data_type K_BINARY? K_ZEROFILL? column_constraint* (K_ON K_UPDATE K_CURRENT_TIMESTAMP)?
   (K_AFTER identifier)? 
   (K_COMMENT (STRING_LITERAL | DOUBLE_QUOTED_LITERAL))? 
 ;
 
colmn_name
 : any_name
 ;

data_type
 : (data_type_nothing
 | data_type_length
 | data_type_length_scale
 | enum_type) (K_UNSIGNED | K_SIGNED)? 
 ;
 
data_type_nothing
 : any_name;
 
data_type_length
 : any_name '(' signed_number ')' 
 ;
 
data_type_length_scale
 : any_name '(' signed_number ',' signed_number ')'
 ;

enum_type
 : any_name enum_type_value
 ;
 
enum_type_value
 : '(' pattern ( ',' pattern )* ')'
 ;
 
column_constraint
 : column_constraint_nullable
 | column_constraint_default
 | column_constraint_auto_increment
 | column_constraint_primary_key
 | column_constraint_collate
 | column_constraint_character_set
 ;
 
column_constraint_nullable: K_NOT? K_NULL;

column_constraint_default: K_DEFAULT (signed_number | literal_value | K_CURRENT_TIMESTAMP);
  
column_constraint_auto_increment: K_AUTO_INCREMENT; 
 
column_constraint_primary_key: K_PRIMARY K_KEY;

column_constraint_collate: K_COLLATE any_name;

column_constraint_character_set: K_CHARACTER K_SET any_name;

primary_key_def
 : K_PRIMARY K_KEY '(' index_columns ')' index_type?
 ;

index_type : K_USING WORD;
 
index_def
 : K_UNIQUE? K_FULLTEXT? (K_INDEX | K_KEY) identifier? '(' index_columns ')' index_type?
 ;
 
index_columns
 : index_column (',' index_column)*
 ;
 
index_column
 : identifier ( '(' signed_number ')' )? 
 ;
 
constraint_def
 : ((K_CONSTRAINT identifier K_FOREIGN K_KEY) | (K_CONSTRAINT? K_FOREIGN K_KEY identifier?))  
   '(' columns ')' K_REFERENCES table_name_ '(' columns ')' cascade_option*
 ;

cascade_option
 : K_ON K_DELETE (K_CASCADE | K_NO K_ACTION)
 | K_ON K_UPDATE (K_CASCADE | K_NO K_ACTION)
 ;
  
table_options
 : table_option? (','? table_option)* 
 ;
 
table_option
 : table_option_name '='? table_option_value
 ;
 
table_option_name
 : identifier+
 | K_AUTO_INCREMENT
 | K_ENGINES
 | K_DEFAULT? K_CHARACTER K_SET
 | K_DEFAULT? K_CHARSET
 | K_COLLATE
 | K_COMMENT 
 ;
 
table_option_value
 : literal_value
 | any_name
 | K_BINARY
 ;
 
create_table_like_stmt
 : K_CREATE K_TABLE table_name_ K_LIKE table_name_
 ;
 
create_user_stmt
 : K_CREATE K_USER (K_IF K_NOT K_EXISTS)? string_value K_IDENTIFIED K_BY string_value
 ;
  
delete_stmt
 : K_DELETE table_name_? from_clause ( K_WHERE expr )? limit_clause?
 ;

drop_database_stmt: K_DROP (K_DATABASE | K_SCHEMA) (K_IF K_EXISTS)? identifier;

drop_table_stmt: K_DROP K_TEMPORARY? K_TABLE (K_IF K_EXISTS)? table_names_ ;

drop_user_stmt : K_DROP K_USER (K_IF K_EXISTS)? string_value;

drop_index_stmt: K_DROP K_INDEX index_name K_ON table_name_ ;

explain_stmt: K_EXPLAIN (K_PROFILE | K_ANALYZE)? sql_stmt;
 
kill_stmt: K_KILL (K_CONNECTION | K_QUERY) number_value; 

load_data_infile_stmt: K_LOAD K_DATA K_INFILE file_name K_INTO K_TABLE table_name_;

file_name: STRING_LITERAL;
 
lock_table_stmt: K_LOCK K_TABLES lock_table_item (',' lock_table_item)*;

lock_table_item
 : table_name_ (K_READ | K_WRITE)?  K_LOCAL?
 ;
 
insert_stmt
 : (K_INSERT | K_REPLACE) HINT? K_IGNORE? K_INTO table_name_ 
   (insert_stmt_values | insert_stmt_select)
   insert_duplicate_clause? 
 ;
 
insert_stmt_values
 : insert_stmt_values_columns? K_VALUES insert_stmt_values_row ( ',' insert_stmt_values_row )*
 ;

insert_stmt_values_columns
 : '(' column_name ( ',' column_name )* ')'
 ;
 
insert_stmt_values_row
 : '(' (expr ( ',' expr )*)? ')'
 ;
 
insert_duplicate_clause
 : K_ON K_DUPLICATE K_KEY K_UPDATE update_stmt_set ( ',' update_stmt_set)*
 ;
   
insert_stmt_select
 : insert_stmt_values_columns? select_stmt
 ;
 
set_stmt
 : K_SET K_OPTION? (set_stmt_variable | set_stmt_names | set_stmt_character_set)
 ;

set_stmt_names
 : K_NAMES names_value (name_collate)?
 ;

set_stmt_character_set
 : K_CHARACTER K_SET names_value
 ;
 
name_collate
 : K_COLLATE names_value
 ;
 
names_value
 : STRING_LITERAL
 | WORD
 | DOUBLE_QUOTED_LITERAL
 ; 
 
set_stmt_variable
 : K_PERMANENT? variable_assignment (',' variable_assignment)* 
 ;
 
rollback_stmt
 : K_ROLLBACK
 ;
 
select_stmt
 : select_or_values ( compound_operator select_or_values )* order_by_clause? limit_clause? ( K_FOR K_UPDATE )?
   K_LOCK? K_IN? K_SHARE? K_MODE?
 ;

compound_operator
 : K_UNION
 | K_UNION K_ALL
 | K_INTERSECT
 | K_EXCEPT
 ;
 
select_or_values
 : K_SELECT HINT? K_SQL_NO_CACHE? 'SQL_CACHE'? K_STRAIGHT_JOIN? ( K_DISTINCT | K_ALL )? select_columns 
   from_clause ? 
   where_clause ? 
   group_by_clause ?
   having_clause ? 
 ;

select_columns
 : result_column ( ',' result_column )*
 ;
 
from_clause
 : from_clause_standard | from_clause_odbc
 ;
 
from_clause_standard
 : K_FROM  from_item ( ',' from_item )* join_clause?
 ;

from_clause_odbc
 : K_FROM '{' K_OJ from_item_odbc '}'
 ;

from_item_odbc
 : from_item
 | '(' from_item_odbc ')'
 | from_item_odbc join_operator from_item join_constraint
 ;
  
limit_clause
 : K_LIMIT number_value ( ',' number_value)? (K_OFFSET number_value)?
 ;
  
where_clause
 : K_WHERE expr
 ;
 
order_by_clause
 : K_ORDER K_BY ordering_term ( ',' ordering_term )*
 ;
 
ordering_term
 : expr ( K_COLLATE collation_name )? ( K_ASC | K_DESC )?
 ;

group_by_clause
 : K_GROUP K_BY expr ( ',' expr )*
 ;

having_clause
 : K_HAVING expr
 ;
 
result_column
 : result_column_star
 | result_column_expr
 ;

result_column_star
 : ( table_name_ '.')? '*'
 ;

result_column_expr
 : expr ( K_AS? column_alias )?
 ;

column_alias
 : identifier
 ;
 
from_item
 : ( from_table index_hint_list? | from_subquery) ( K_AS? table_alias )?
 ;

index_hint_list: index_hint+;

index_hint
 : K_USE (K_INDEX | K_KEY) '(' index_list ')'
 | K_IGNORE (K_INDEX | K_KEY) '(' index_list ')'
 | K_FORCE (K_INDEX | K_KEY) '(' index_list ')'
 ;

index_list: index_name ( ',' index_name)*;

from_table
 : table_name_
 ;
 
from_subquery
 : '(' select_stmt ')'
 ;
  
join_clause
 : join_item+
 ;

join_item
 : join_operator from_item join_constraint
 ;
 
join_operator
 : (K_LEFT | K_RIGHT)? (K_OUTER | K_INNER | K_CROSS)? (K_JOIN | K_STRAIGHT_JOIN)
 ;

join_constraint
 : ( K_ON expr
   | K_USING '(' column_name ( ',' column_name )* ')' )?
 ;

show_charset
 : K_SHOW (K_CHARSET | K_CHARACTER K_SET)
 ;
 
show_collation
 : K_SHOW K_COLLATION
 ;
 
show_databases
 : K_SHOW K_DATABASES
 ;

show_engines
 : K_SHOW K_ENGINES
 ;

show_function_stmt : K_SHOW K_FUNCTION K_STATUS ( K_LIKE string_value )? where_clause? ;
 
show_grants
 : K_SHOW K_GRANTS
 ;
 
show_master_status
 : K_SHOW K_MASTER K_STATUS
 ;
  
show_privileges
 : K_SHOW K_PRIVILEGES
 ;

show_procedure
 : K_SHOW K_PROCEDURE K_STATUS ( K_LIKE string_value )? where_clause? 
 ;
   
show_processlist
 : K_SHOW K_FULL? K_PROCESSLIST
 ;
 
show_status
 : K_SHOW K_GLOBAL? K_SESSION? K_STATUS ( K_LIKE string_value )?
 ;
  
show_index_stmt
 : K_SHOW (K_INDEX | K_INDEXES | K_KEYS) (K_FROM | K_IN) table_name_ ((K_FROM | K_IN) identifier)?
 ;

show_table_status_stmt
 : K_SHOW K_TABLE K_STATUS ( K_FROM identifier )? ( K_LIKE string_value )? where_clause?
 ;
  
show_tables_stmt
 : K_SHOW K_FULL? K_TABLES ( K_FROM identifier )? (K_LIKE string_value)? where_clause?
 ;
  
show_triggers_stmt
 : K_SHOW K_FULL? K_TRIGGERS ( K_FROM identifier )? (K_LIKE string_value)? where_clause?
 ;
 
show_columns_stmt
 : K_SHOW K_FULL? (K_COLUMNS | K_FIELDS) K_FROM table_name_ (K_FROM identifier)? (K_LIKE string_value)?
 ;

show_create_table_stmt
 : K_SHOW K_CREATE K_TABLE table_name_
 ;
   
show_variable_stmt
 : K_SHOW K_GLOBAL? K_SESSION? K_VARIABLES (where_clause | show_variable_like_clause)?  
 ;

show_variable_like_clause
 : K_LIKE literal_value
 ;
 
show_warnings_stmt
 : K_SHOW K_WARNINGS
 ;
 
truncate_table_stmt
 : K_TRUNCATE K_TABLE? table_name_
 ;

unlock_table_stmt: K_UNLOCK K_TABLES;

update_stmt
 : with_clause? K_UPDATE table_name_ K_SET update_stmt_set ( ',' update_stmt_set)* ( K_WHERE expr )? limit_clause?
 ;

update_stmt_set
 : column_name '=' expr
 ;

use_stmt: K_USE identifier;

variable_assignment
 : variable_assignment_user
 | variable_assignment_session
 | variable_assignment_global
 | variable_assignment_transaction
 | variable_assignment_session_transaction
 | variable_assignment_global_transaction
 ;
 
variable_assignment_session
 : K_SESSION? session_variable_name '=' set_expr
 ;
 
session_variable_name
 : SESSION_VARIABLE | any_name
 ;
 
variable_assignment_global
 : K_GLOBAL any_name '=' set_expr
 ;
 
variable_assignment_user
 : user_var_name '=' expr
 ;

variable_assignment_session_transaction
 : K_SESSION transaction_set
 ;
 
variable_assignment_global_transaction
 : K_GLOBAL transaction_set
 ;
 
variable_assignment_transaction
 : transaction_set
 ;

transaction_set
 : K_TRANSACTION transaction_characteristic ( ',' transaction_characteristic)*
 ;

transaction_characteristic
 : K_ISOLATION K_LEVEL level
 | K_READ K_WRITE
 | K_READ K_ONLY
 ;
 
level
 : K_READ K_COMMITTED
 | K_READ K_UNCOMMITTED
 | K_REPEATABLE K_READ
 | K_SERIALIZABLE
 ;
 
set_expr
 : K_DEFAULT | names_value | expr
 ;
 
user_var_name
 : USER_VARIABLE
 ;
 
with_clause
 : K_WITH K_RECURSIVE? table_name K_AS '(' select_stmt ')' ( ',' table_name K_AS '(' select_stmt ')' )*
 ;

value
 : literal_value
 | bind_parameter
 | column_name_
 | variable_reference
 | session_variable_reference
 ;

expr
 : value
 | expr_search
 | expr_exist 
 | expr_function
 | expr_parenthesis
 | expr_select 
 | expr_unary
 | expr '||' expr
 | expr ( '*' | '/' | '%' | K_MOD) expr
 | expr ( '+' | '-' ) expr
 | expr ( '<<' | '>>' | '&' | '|' ) expr
 | expr ( '<' | '<=' | '>' | '>=' ) expr
 | expr ( '=' | '==' | '!=' | '<>' | '<=>' | K_IS K_NOT | K_IS | K_GLOB | K_MATCH ) expr
 | expr K_NOT? ( K_LIKE | K_GLOB | K_REGEXP | K_MATCH ) like_expr ( K_ESCAPE expr )?
 | expr expr_in_select
 | expr expr_in_values
 | expr_simple K_NOT? K_BETWEEN expr_simple K_AND  expr_simple
 | expr_not
 | expr K_AND expr
 | expr K_REGEXP pattern
 | expr K_OR expr
 | expr ( K_ISNULL | K_NOTNULL | K_NOT K_NULL )
 | expr K_IS K_NOT? expr
 | expr_case
 | K_DISTINCT expr
 | expr_cast
 ;

expr_primary
 : value 
 | expr_function
 | expr_parenthesis
 ;
 
expr_not : (K_NOT | '!') expr_relational;

expr_relational
 : expr_compare
 | expr_between
 | expr_is
 | expr_match
 | expr_numeric expr_in_select
 | expr_numeric expr_in_values
 | expr_numeric
 ;

expr_compare : expr_numeric ( '<' | '<=' | '>' | '>=' | '=' | '!=' | '<>' | '<=>') expr_numeric;

expr_between : expr_numeric K_NOT? K_BETWEEN expr_numeric K_AND  expr_numeric;

expr_is : expr_numeric K_IS K_NOT? K_NULL;

expr_match : expr_numeric K_NOT? ( K_LIKE | K_REGEXP ) like_expr ( K_ESCAPE expr_numeric )?;

expr_numeric : expr_additive ;

expr_additive : expr_multi (('+' | '-' | '|') expr_multi)*;

expr_multi : expr_unary (('*' | '/' | '&' |  '%' | K_MOD) expr_unary)*;
     
expr_unary : ('-' | '+' | '~' | K_BINARY)? expr_primary;

expr_simple
 : literal_value
 | bind_parameter
 | column_name_
 | variable_reference
 | session_variable_reference
 | expr_function
 | expr_select 
 | expr_simple '||' expr_simple
 | expr_simple ( '*' | '/' | '%' | K_MOD) expr_simple
 | expr_simple ( '+' | '-' ) expr_simple
 ;

expr_case : K_CASE expr? expr_case_when+ expr_case_else? K_END;

expr_case_when : K_WHEN expr K_THEN expr;

expr_case_else : K_ELSE expr;

like_expr
 : expr_simple
 ;
 
pattern
 : STRING_LITERAL | DOUBLE_QUOTED_LITERAL 
 ;
 
column_name_
 : (identifier '.')* column_name
 ;
 
bind_parameter
 : BIND_PARAMETER
 ;
 
expr_cast
 : K_CAST '(' expr  K_AS expr_cast_data_type ')' ?
 ;

expr_cast_data_type
 : (K_SIGNED | K_UNSIGNED) ? any_name? ('(' signed_number (',' signed_number )* ')')?
 ;
  
expr_function
 : function_name '(' (expr_function_parameters | expr_function_star_parameter | group_concat_parameter)? ')'
 ;

group_concat_parameter
 : K_DISTINCT? expr ( K_ORDER K_BY ordering_term ( ',' ordering_term )* )?  (K_SEPARATOR literal_value)?
 ;
 
expr_function_parameters
 : expr ( ',' expr )*
 ;

expr_function_star_parameter
 : '*'
 ; 
  
expr_search
 : K_MATCH '(' column_name_ ( ',' column_name_)* ')' K_AGAINST '(' value (K_IN K_BOOLEAN K_MODE)? ')'
 ;
 
expr_parenthesis
 : '(' expr ')'
 ;
  
expr_select
 : '('select_stmt ')'
 ;
 
expr_exist
 : K_NOT? K_EXISTS '(' select_stmt ')'
 ;

expr_in_select
 : K_NOT? K_IN '('select_stmt ')'
 ;

expr_in_values
 : K_NOT? K_IN '(' expr ( ',' expr )* ')'
 ;
 
variable_reference: USER_VARIABLE;

session_variable_reference: SESSION_VARIABLE;

database_name
 : any_name
 ;

table_name: ( any_name '.' )? any_name ;

column_name 
 : identifier
 ;

index_name : identifier;

indexed_column : identifier;

table_name_
 : ( identifier '.' )? identifier
 ;

table_names_
 : table_name_ (',' table_name_)*
 ;

collation_name 
 : any_name
 ;

table_alias 
 : identifier
 ;

function_name
 : any_name | K_LEFT | K_IF | K_MOD | K_ISNULL | K_REPLACE
 ;

any_name
 : name 
 ;

name
 : WORD | K_DATABASE | K_DATABASES | K_ENGINES | K_COLLATION | K_DATA | K_LEVEL | K_DESC | K_READ | K_COMMENT 
 | K_MATCH | K_BINARY | K_TABLES | K_AUTO_INCREMENT | K_GRANTS | K_COLUMNS | K_SESSION | K_ATTACH | K_PROFILE
 | K_MATCH | K_AGAINST | K_BOOLEAN | K_MODE | K_STATUS | K_PROCESSLIST | K_PRIVILEGES | K_LOCAL | K_USER
 | K_IDENTIFIED | K_PERMANENT | K_KILL | K_CONNECTION | K_QUERY | K_DUPLICATE | K_FORCE | K_OPTION | K_SHARE
 | K_ZEROFILL | K_PROCEDURE | K_TRIGGERS | K_VARIABLES | K_ACTION | K_NO | K_FUNCTION | K_OJ | K_ANALYZE
 ;
 
identifier
 : DOUBLE_QUOTED_LITERAL | BACKTICK_QUOTED_IDENTIFIER | name
 ;

signed_number
 : ( '+' | '-' )? NUMERIC_LITERAL
 ;

number_value:NUMERIC_LITERAL;

string_value:STRING_LITERAL;

blob_value:BLOB_LITERAL;

hex_value:HEX_LITERAL;

null_value:K_NULL;

current_time_value:K_CURRENT_TIME;

current_date_value:K_CURRENT_DATE;

current_timestamp_value:K_CURRENT_TIMESTAMP;

literal_value
 : NUMERIC_LITERAL
 | STRING_LITERAL
 | DOUBLE_QUOTED_LITERAL
 | BLOB_LITERAL
 | HEX_LITERAL
 | literal_value_binary
 | literal_interval
 | K_NULL
 | K_CURRENT_TIME
 | K_CURRENT_DATE
 | K_CURRENT_TIMESTAMP
 | K_TRUE
 | K_FALSE
 ;

literal_interval
 : K_INTERVAL expr WORD
 ;
 
literal_value_binary
 : K__BINARY STRING_LITERAL
 ;
 
EXCLAIMATION : '!';
SCOL : ';';
DOT : '.';
OPEN_PAR : '(';
CLOSE_PAR : ')';
COMMA : ',';
ASSIGN : '=';
STAR : '*';
PLUS : '+';
MINUS : '-';
TILDE : '~';
PIPE2 : '||';
DIV : '/';
MOD : '%';
LT2 : '<<';
GT2 : '>>';
AMP : '&';
PIPE : '|';
LT : '<';
LT_EQ : '<=';
GT : '>';
GT_EQ : '>=';
EQ : '==';
NOT_EQ1 : '!=';
NOT_EQ2 : '<>';

// http://www.sqlite.org/lang_keywords.html
K_ABORT : A B O R T;
K_ACTION : A C T I O N;
K_ADD : A D D;
K_AFTER : A F T E R;
K_AGAINST : A G A I N S T;
K_ALL : A L L;
K_ALTER : A L T E R;
K_ANALYZE : A N A L Y Z E;
K_AND : A N D;
K_AS : A S;
K_ASC : A S C;
K_ATTACH : A T T A C H;
K_AUTO_INCREMENT : A U T O '_' I N C R E M E N T;
K_BEFORE : B E F O R E;
K_BEGIN : B E G I N;
K_BETWEEN : B E T W E E N;
K__BINARY : '_' B I N A R Y;
K_BINARY : B I N A R Y;
K_BOOLEAN : B O O L E A N;
K_BY : B Y;
K_CASCADE : C A S C A D E;
K_CASE : C A S E;
K_CAST : C A S T;
K_CHANGE : C H A N G E;
K_CHARACTER : C H A R A C T E R;
K_CHARSET : C H A R S E T;
K_CHECK : C H E C K;
K_COLLATE : C O L L A T E;
K_COLUMN : C O L U M N;
K_COLUMNS : C O L U M N S;
K_COLLATION : C O L L A T I O N;
K_COMMENT : C O M M E N T;
K_COMMIT : C O M M I T;
K_CONFLICT : C O N F L I C T;
K_CONNECTION : C O N N E C T I O N;
K_CONSTRAINT : C O N S T R A I N T;
K_CREATE : C R E A T E;
K_CROSS : C R O S S;
K_CURRENT_DATE : C U R R E N T '_' D A T E;
K_CURRENT_TIME : C U R R E N T '_' T I M E;
K_CURRENT_TIMESTAMP : C U R R E N T '_' T I M E S T A M P;
K_DATA : D A T A;
K_DATABASE : D A T A B A S E;
K_DATABASES : D A T A B A S E S;
K_DEFAULT : D E F A U L T;
K_DEFERRABLE : D E F E R R A B L E;
K_DEFERRED : D E F E R R E D;
K_DELETE : D E L E T E;
K_DESC : D E S C;
K_DETACH : D E T A C H;
K_DISABLE : D I S A B L E;
K_DISTINCT : D I S T I N C T;
K_DROP : D R O P;
K_DUPLICATE : D U P L I C A T E;
K_EACH : E A C H;
K_ELSE : E L S E;
K_ENABLE : E N A B L E;
K_END : E N D;
K_ENGINES : E N G I N E S;
K_GRANTS : G R A N T S;
K_ESCAPE : E S C A P E;
K_EXCEPT : E X C E P T;
K_EXCLUSIVE : E X C L U S I V E;
K_EXISTS : E X I S T S;
K_EXPLAIN : E X P L A I N;
K_FAIL : F A I L;
K_FALSE : F A L S E;
K_FIELDS : F I E L D S;
K_FOR : F O R;
K_FORCE : F O R C E;
K_FOREIGN : F O R E I G N;
K_FROM : F R O M;
K_FUNCTION : F U N C T I O N;
K_FULL : F U L L;
K_FULLTEXT : F U L L T E X T;
K_GLOB : G L O B;
K_GLOBAL : G L O B A L;
K_GROUP : G R O U P;
K_HAVING : H A V I N G;
K_IDENTIFIED : I D E N T I F I E D;
K_IF : I F;
K_IGNORE : I G N O R E;
K_IMMEDIATE : I M M E D I A T E;
K_IN : I N;
K_INDEX : I N D E X;
K_INDEXES : I N D E X E S;
K_INDEXED : I N D E X E D;
K_INFILE : I N F I L E;
K_INITIALLY : I N I T I A L L Y;
K_INNER : I N N E R;
K_INSERT : I N S E R T;
K_INSTEAD : I N S T E A D;
K_INTERSECT : I N T E R S E C T;
K_INTERVAL : I N T E R V A L;
K_INTO : I N T O;
K_IS : I S;
K_ISNULL : I S N U L L;
K_JOIN : J O I N;
K_KEY : K E Y;
K_KEYS : K E Y S;
K_KILL : K I L L;
K_LEFT : L E F T;
K_LIKE : L I K E;
K_LIMIT : L I M I T;
K_LOAD : L O A D;
K_LOCAL : L O C A L;
K_LOCK : L O C K;
K_MASTER : M A S T E R;
K_MATCH : M A T C H;
K_MOD : M O D;
K_MODE : M O D E;
K_MODIFY : M O D I F Y;
K_NAMES : N A M E S;
K_NATURAL : N A T U R A L;
K_NO : N O;
K_NOT : N O T;
K_NOTNULL : N O T N U L L;
K_NULL : N U L L;
K_OF : O F;
K_OFFSET : O F F S E T;
K_OJ : O J;
K_ON : O N;
K_OPTION : O P T I O N;
K_OR : O R;
K_ORDER : O R D E R;
K_OUTER : O U T E R;
K_PERMANENT : P E R M A N E N T;
K_PLAN : P L A N;
K_PRAGMA : P R A G M A;
K_PRIMARY : P R I M A R Y;
K_PRIVILEGES : P R I V I L E G E S;
K_PROCEDURE : P R O C E D U R E;
K_PROCESSLIST : P R O C E S S L I S T;
K_PROFILE : P R O F I L E;
K_QUERY : Q U E R Y;
K_RAISE : R A I S E;
K_RECURSIVE : R E C U R S I V E;
K_REFERENCES : R E F E R E N C E S;
K_REGEXP : R E G E X P;
K_REINDEX : R E I N D E X;
K_RELEASE : R E L E A S E;
K_RENAME : R E N A M E;
K_REPLACE : R E P L A C E;
K_RESTRICT : R E S T R I C T;
K_RIGHT : R I G H T;
K_ROLLBACK : R O L L B A C K;
K_ROW : R O W;
K_ROWNUM : R O W N U M;
K_SAVEPOINT : S A V E P O I N T;
K_SCHEMA : S C H E M A;
K_SELECT : S E L E C T;
K_SEPARATOR : S E P A R A T O R; 
K_SEQUENCE: S E Q U E N C E;
K_SESSION: S E S S I O N;
K_SET : S E T;
K_SHARE : S H A R E;
K_SHOW : S H O W;
K_SIGNED : S I G N E D;
K_SQL_NO_CACHE : S Q L '_' N O '_' C A C H E;
K_START : S T A R T;
K_STATUS : S T A T U S;
K_STOP : S T O P;
K_STRAIGHT_JOIN : S T R A I G H T '_' J O I N;
K_SLAVE : S L A V E;
K_TABLE : T A B L E;
K_TABLES : T A B L E S;
K_TEMPORARY : T E M P O R A R Y;
K_THEN : T H E N;
K_TO : T O;
K_TRANSACTION : T R A N S A C T I O N;
K_TRIGGER : T R I G G E R;
K_TRIGGERS : T R I G G E R S;
K_TRUE : T R U E;
K_TRUNCATE : T R U N C A T E;
K_UNION : U N I O N;
K_UNIQUE : U N I Q U E;
K_UNLOCK : U N L O C K;
K_UNSIGNED : U N S I G N E D;
K_UPDATE : U P D A T E;
K_USE : U S E;
K_USER : U S E R;
K_USING : U S I N G;
K_VACUUM : V A C U U M;
K_VALUES : V A L U E S;
K_VARIABLES : V A R I A B L E S;
K_VIEW : V I E W;
K_VIRTUAL : V I R T U A L;
K_WHEN : W H E N;
K_WHERE : W H E R E;
K_WITH : W I T H;
K_WITHOUT : W I T H O U T;
K_ZEROFILL : Z E R O F I L L;
K_ISOLATION : I S O L A T I O N;
K_LEVEL : L E V E L;
K_READ : R E A D;
K_UNCOMMITTED : U N C O M M I T T E D;
K_COMMITTED : C O M M I T T E D;
K_WARNINGS : W A R N I N G S;
K_WRITE : W R I T E;
K_ONLY : O N L Y;
K_REPEATABLE : R E P E A T A B L E;
K_SERIALIZABLE : S E R I A L I Z A B L E;
K_HEX : '0' X;
K___DELETE : '_' '_' D E L E T E;

USER_VARIABLE: '@' [a-zA-Z_] [a-zA-Z_0-9]* ;
 
SESSION_VARIABLE: '@' '@' [$.a-zA-Z_] [$.a-zA-Z_0-9]* ;
 
WORD
 : [a-zA-Z_] [a-zA-Z_0-9\u00c0-\u00ff]*
 ;
 
BACKTICK_QUOTED_IDENTIFIER
 : '`' (~'`' | '``')* '`' 
 ;
    
NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;

BIND_PARAMETER
 : '?' DIGIT*
 ;

STRING_LITERAL
 : '\'' ( ~('\''|'\\') | ('\\' .) | ('\'' '\'') )* '\''
 ;

DOUBLE_QUOTED_LITERAL
 : '"' ( ~'"')* '"'
 ;
 
BLOB_LITERAL
 : X STRING_LITERAL
 ;

HEX_LITERAL
 : K_HEX [A-Z0-9]*
 ;

SINGLE_LINE_COMMENT
 : '--' ~[\r\n]* -> channel(HIDDEN)
 ;

SINGLE_LINE_COMMENT2
 : '#' ~[\r\n]* -> channel(HIDDEN)
 ;

MULTILINE_COMMENT
 : '/*' .*? ( '*/' | EOF )  -> channel(HIDDEN)
 ;

HINT
 : '/*' (~ '*')+ '*/'
 ;
 
SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];