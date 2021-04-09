@echo off

set allparam=

:param
set str=%1
if "%str%"=="" (
    goto end
)
set allparam=%allparam% %str%
shift /0
goto param

:end
if "%allparam%"=="" (
    goto eof
)

rem remove left right blank
:intercept_left
if "%allparam:~0,1%"==" " set "allparam=%allparam:~1%"&goto intercept_left

:intercept_right
if "%allparam:~-1%"==" " set "allparam=%allparam:~0,-1%"&goto intercept_right


:eof
set bash=../setup/target/dist/jsw/antsdb

set ANTSDB_LIBS=%bash%\lib\*

if not exist "%ANTSDB_LIBS%" goto :eof
 
for /F %%F in ('dir /A:D /b "%ANTSDB_LIBS%"') do (
	if not "%%F" == "optional" call :concat "%bash%\lib\%%F\*"
)

if defined USER_LIBS set ANTSDB_LIBS=%USER_LIBS%;%ANTSDB_LIBS%

set CP=target/classes;%ANTSDB_LIBS%
java -cp %CP%  com.antsdb.saltedfish.obs.ObjectStoreRelinkMain %allparam%
 