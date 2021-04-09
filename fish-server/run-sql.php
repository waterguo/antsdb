<?php
## check api existence
if (!function_exists("mysql_pconnect")) {
	printf("function mysql_pconnect() is not found, install php56-mysql\n");
	die();  
}
if (!function_exists("mb_internal_encoding")) {
	printf("function mb_internal_encoding() is not found, install php56-mbstring\n");
	die();  
}

## parse command line arguments
$host = '';
$port = '3306';
$user = '';
$password = '';
$sql = '';
for ($i=1; $i<sizeof($argv); $i++) {
	$ii = $argv[$i];
	if ($ii == '-h' || $ii == '--host') {
		$host = $argv[$i+1];
		$i++;
	}
	else if ($ii == '--port') {
		$port = $argv[$i+1];
		$i++;
	}
	else if ($ii == '-u' || $ii == '--user') {
		$user = $argv[$i+1];
		$i++;
	}
	else if ($ii == '-p' || $ii == '--password') {
		$password = $argv[$i+1];
		$i++;
	}
	else {
		$sql = $ii;
	}
}


# connect to datatbase
$conn = mysql_pconnect($host.":".$port, $user, $password);
printf("current php version: %s\n", phpversion());
printf("current encoding: %s\n", mb_internal_encoding());
printf("mysql client encoding: %s\n", mysql_client_encoding($conn));
printf("mysql client info: %s\n", mysql_get_client_info());
printf("mysql server info: %s\n", mysql_get_server_info());
printf("mysql host info: %s\n", mysql_get_host_info());
printf("mysql protocol version: %s\n", mysql_get_proto_info());

# query 
printf("query: %s\n", $sql);
$result = mysql_query($sql);
if (!$result) {
    die('Invalid query: ' . mysql_error());
}
if ($result === true) {
	printf("done\n");
	die();
}
printf("===========================================================================================================\n");
$temp;
while ($row = mysql_fetch_row($result)) {
	if ($temp)
		printf("-----------------------------------------------------------------------------------------------------------\n");
	$temp = $row;
	for ($i=0; $i<sizeof($row); $i++) {
		printf("%s: %s\n", mysql_field_name($result, $i), $row[$i]);
	}
}
?>