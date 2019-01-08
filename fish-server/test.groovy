#! /usr/bin/env groovy

@GrabConfig( systemClassLoader=true )
@GrabResolver(name='lepton', root='http://lepton.iceup.net/repos')
@Grab('com.oracle:ojdbc6:11.2.0.4')
@Grab('com.oracle:xdb6:11.2.0.4')
@Grab('mysql:mysql-connector-java:5.1.28')

import java.sql.*

conn = getConnection_mysql()
conn.autoCommit = false
def pstat = conn.prepareStatement("insert into asfasfk values (?, ?, ?)")
System.console().readLine "press enter to close"
pstat.setInt(1, 1)
pstat.setInt(2, 11)
pstat.setInt(3, 111)
pstat.executeUpdate();

def getConnection_mysql() {
    Class.forName('com.mysql.jdbc.Driver')
    def conn = DriverManager.getConnection("jdbc:mysql://nas:3306/test","test","test")
}

def getConnection_oracle() {
    Class.forName('oracle.jdbc.driver.OracleDriver')
    def conn = DriverManager.getConnection('jdbc:oracle:thin:@oracle:1521:orcl', 'sonar', 'sonar')
}
