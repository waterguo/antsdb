set bash=../setup/target/dist/jsw/antsdb

set ANTSDB_LIBS=%bash%\lib\*

if not exist "%ANTSDB_LIBS%" goto :eof
 
for /F %%F in ('dir /A:D /b "%ANTSDB_LIBS%"') do (
	if not "%%F" == "optional" call :concat "%bash%\lib\%%F\*"
)

if defined USER_LIBS set ANTSDB_LIBS=%USER_LIBS%;%ANTSDB_LIBS%

set CP=target/classes;%ANTSDB_LIBS%
java -cp %CP%  com.antsdb.saltedfish.backup.JdbcBackupMain %*
