grammar Fish;

script
 : stmt (';' stmt?)* EOF
 ; 

stmt
  : add_master_node_stmt
  | carbonfreeze_stmt
  | change_slave_stmt
  | clear_meta_cache_stmt
  | delete_node_stmt
  | evict_cache_stmt
  | ping_stmt
  | gc_stmt
  | hdelete_stmt
  | hselect_stmt
  | hupdate_stmt
  | join_cluster_stmt
  | leave_cluster_stmt
  | reorganize_stmt
  | show_cluster_status_stmt
  | shutdown_stmt
  | start_replicator_stmt
  | start_slave_stmt
  | start_synchronizer_stmt
  | stop_replicator_stmt
  | stop_slave_stmt
  | stop_synchronizer_stmt
  | sync_stmt
  | reset_metrics_stmt
  ; 

add_master_node_stmt: K_ADD K_EXTERNAL? (K_MASTER | K_SLAVE) K_NODE assignment*;

carbonfreeze_stmt: K_CARBONFREEZE;

evict_cache_stmt: K_EVICT K_CACHE (cache_page | cache_table | cache_all);

cache_page: K_PAGE number_value;

cache_table: K_TABLE number_value;

cache_all: K_ALL;

clear_meta_cache_stmt : K_CLEAR K_META K_CACHE;

change_slave_stmt: K_CHANGE K_SLAVE K_TO assignment*;
 
delete_node_stmt: K_DELETE K_NODE STRING_LITERAL;

gc_stmt: K_GC;
  
ping_stmt: K_PING;

reorganize_stmt: K_REORGANIZE;

hdelete_stmt: K_HDELETE K_FROM table where;

hselect_stmt: K_HSELECT columns K_FROM table where? limit?;

hupdate_stmt: K_HUPDATE table update_sets where;

join_cluster_stmt: K_JOIN K_CLUSTER K_AS (K_MASTER | K_SLAVE) assignment*;

leave_cluster_stmt : K_LEAVE K_CLUSTER;

update_sets: K_SET update_set ( ', ' update_set)*;

update_set: K_SET column '=' value;

where: K_WHERE expr;

limit: K_LIMIT number_value (K_OFFSET number_value)?;
 
expr
 : expr_equal
 | expr K_AND expr
 ; 

expr_equal: column '=' value;

value: STRING_LITERAL | NUMERIC_LITERAL | K_NULL;
 
columns: column+;

column: ('$' number_value) | '*';

table: number_value;

show_cluster_status_stmt: K_SHOW K_CLUSTER K_STATUS;

shutdown_stmt: K_SHUTDOWN;

start_replicator_stmt: K_START K_REPLICATOR;

start_slave_stmt : K_START K_SLAVE; 

start_synchronizer_stmt: K_START K_SYNCHRONIZER;

stop_replicator_stmt: K_STOP K_REPLICATOR;

stop_slave_stmt: K_STOP K_SLAVE;

stop_synchronizer_stmt : K_STOP K_SYNCHRONIZER;
     
sync_stmt: K_SYNCHRONIZE;

reset_metrics_stmt: K_RESET K_METRICS;

number_value:NUMERIC_LITERAL;

assignment: IDENTIFIER '=' STRING_LITERAL;

K_ADD: A D D;
K_ALL: A L L;
K_AND: A N D;
K_AS: A S;
K_CACHE : C A C H E;
K_CARBONFREEZE : C A R B O N F R E E Z E;
K_CHANGE : C H A N G E; 
K_CLEAR : C L E A R;
K_DELETE : D E L E T E;
K_EVICT : E V I C T;
K_EXTERNAL : E X T E R N A L;
K_FROM : F R O M;
K_GC : G C;
K_CLUSTER: C L U S T E R;
K_LEAVE : L E A V E;
K_LIMIT : L I M I T;
K_MASTER: M A S T E R;
K_META : M E T A;
K_METRICS : M E T R I C S;
K_NULL : N U L L;
K_NODE: N O D E;
K_OFFSET : O F F S E T;
K_PAGE : P A G E;
K_PING : P I N G;
K_REORGANIZE : R E O R G A N I Z E;
K_REPLICATOR : R E P L I C A T O R;
K_RESET : R E S E T;
K_HDELETE : H D E L E T E;
K_HSELECT : H S E L E C T;
K_HUPDATE : H U P D A T E;
K_JOIN : J O I N;
K_SET : S E T;
K_SHOW : S H O W;
K_SHUTDOWN : S H U T D O W N;  
K_SLAVE : S L A V E;
K_START : S T A R T;
K_STATUS : S T A T U S;
K_STOP : S T O P;
K_SYNCHRONIZE : S Y N C H R O N I Z E;
K_SYNCHRONIZER : S Y N C H R O N I Z E R;
K_TABLE : T A B L E;
K_TO : T O;
K_WHERE : W H E R E; 
    
IDENTIFIER : [a-zA-Z_] [a-zA-Z_0-9\u00c0-\u00ff]*;
 
STRING_LITERAL : '\'' ( ~('\''|'\\') | ('\\' .) | ('\'' '\'') )* '\'';

SPACES : [ \u000B\t\r\n] -> channel(HIDDEN);
 
NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
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
