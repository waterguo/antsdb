
/* sequences */
create table __SYS.SYSSEQUENCE (
	id int,
	namespace varchar(255), 
	sequence_name varchar(255),
	last_number _long,
	seed _long,
	increment _long);
/* tables */
create table __SYS.SYSTABLE (
	id int, 
	namespace varchar(255), 
	table_name varchar(255), 
	table_type varchar(255),
	ext_name varchar(255));
/* columns */
create table __SYS.SYSCOLUMN (
	id int,
	column_id int,
	namespace varchar(255), 
	table_name varchar(255), 
	column_name varchar(255), 
	type_name varchar(255),
	type_length int,
	type_scale int,
	nullable _boolean,
	default_value varchar(255));
/* database rules such as primary key, index, foreign key etc */
create table __SYS.SYSRULE (
	id int,
	namespace varchar(255), 
	table_id int, 
	rule_name varchar(255), 
	rule_type int,
	is_unique _boolean);
/* columns that used by the rule */
create table __SYS.SYSRULECOL (
	id int,
	rule_id int,
	column_id int);
/* system parameters */
create table __SYS.SYSPARAM (
	name varchar(255),
	type varchar(20),
	value varchar(1024));