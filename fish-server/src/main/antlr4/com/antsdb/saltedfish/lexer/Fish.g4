grammar Fish;

script
 : stmt (';' stmt?)* EOF
 ; 

stmt
  : carbonfreeze_stmt
  | change_slave_stmt
  | evict_cache_stmt
  | ping_stmt
  | gc_stmt
  | reorganize_stmt
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

carbonfreeze_stmt: K_CARBONFREEZE;

evict_cache_stmt: K_EVICT K_CACHE (cache_page | cache_table | cache_all);

cache_page: K_PAGE number_value;

cache_table: K_TABLE number_value;

cache_all: K_ALL;

change_slave_stmt: K_CHANGE K_SLAVE K_TO assignment*;
 
gc_stmt: K_GC;
  
ping_stmt: K_PING;

reorganize_stmt: K_REORGANIZE;

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

K_ALL: A L L;
K_CACHE : C A C H E;
K_CARBONFREEZE : C A R B O N F R E E Z E;
K_CHANGE : C H A N G E; 
K_CLEAR : C L E A R;
K_EVICT : E V I C T;
K_GC : G C;
K_METRICS : M E T R I C S;
K_PAGE : P A G E;
K_PING : P I N G;
K_REORGANIZE : R E O R G A N I Z E;
K_REPLICATOR : R E P L I C A T O R;
K_RESET : R E S E T;
K_SHUTDOWN : S H U T D O W N;  
K_SLAVE : S L A V E;
K_START : S T A R T;
K_STOP : S T O P;
K_SYNCHRONIZE : S Y N C H R O N I Z E;
K_SYNCHRONIZER : S Y N C H R O N I Z E R;
K_TABLE : T A B L E;
K_TO : T O;
    
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
