/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.antsdb.saltedfish.util.BenchmarkWorker;
import com.antsdb.saltedfish.util.Benchmarker;

public class EscapeAnalysis {
    
    static volatile boolean _stop = false;
    static class Timer implements Runnable {
        int ms;
        
        Timer(int ms) {
            this.ms = ms;
        }
        
        @Override
        public void run() {
            try {
                Thread.sleep(this.ms * 1000l);
            } 
            catch (InterruptedException e) {
            }
            EscapeAnalysis._stop = true;
        }
    }
    
    static class ReturnValue {
        boolean isNull;
        long value;
    }
    
    static abstract class AddBase {
        abstract void add(long x, long y, ReturnValue retVal);
    }
    
    static class Add extends AddBase{
        //byte[] buf = new byte[1000];
        
        int exec(int x, int y) {
            Integer xx = x;
            return xx+y;
        }

        long exec1(long x, long y) {
            //Long xx = Long.valueOf(x);
            Long xx = new Long(x);
            return xx+y;
        }

        @Override
        void add(long x, long y, ReturnValue retVal) {
            retVal.value = x + y;
        }
        
        ReturnValue add1(long x, long y) {
            long n = x + y;
            ReturnValue ret = new ReturnValue();
            ret.value = n;
            return ret;
        }
        
        Long add(Long x, Long y) {
            Long xx = x;
            Long n = xx+y;
            return n;
        }

        BigInteger add2(long x, long y) {
            /*
            BigInteger xx = BigInteger.valueOf(x);
            BigInteger yy = BigInteger.valueOf(y);
            BigInteger n = xx.add(yy);
            */
            BigInteger n = BigInteger.valueOf(x+y);
            return n;
        }
    }
    
    static class TestPrimitiveType implements Callable<Long> {
        long count = 0;
        int blah = 0;
        
        @Override
        public Long call() throws Exception {
            for (;;) {
                if (EscapeAnalysis._stop) {
                    return count;
                }
                op();
                this.count++;
            }
        }
        
        void op() {
            int x = (int)this.blah;
            int y = (int)this.count;
            Add add = new Add();
            this.blah += add.exec(x, y);
            //this.blah += add.hashCode();
        }
        
        long add(long x, long y) {
            //Long xx = x;
            long z = x + y;
            return z;
        }
    }
    
    public static class TestByteBuffer extends BenchmarkWorker {
        long count = 0;
        ByteBuffer buf = ByteBuffer.allocateDirect(1024*1024*10);
        CharBuffer cbuf = buf.asCharBuffer();
        long blah = 0;
        
        public void op() {
            /*
            StringBuilder s = new StringBuilder(100);
            s.append('1');
            s.append('2');
            s.append('3');
            s.append('4');
            s.append('5');
            */
            String s = "blah";
            CharBuffer sbuf = buf.asCharBuffer();
            sbuf.position(0);
            sbuf.put(s);
            //char[] chars = s.toCharArray();
            //blah += chars.length;
            //sbuf.put(s.toCharArray());
        }
        
        // vulnerable to gc
        //char[] chars = new char[] {'A', 'B', 'C', 'D'};
        void write() {
            char[] chars = new char[4];
            chars[0] = 'A';
            chars[1] = 'B';
            chars[2] = 'C';
            chars[3] = 'D';
            this.cbuf.position(0);
            this.cbuf.put(chars);
        }
        
        // vulnerable to gc
        void read() {
            String s = this.buf.asCharBuffer().toString();
            this.blah += s.length();
        }
    }
    
    /**
     * this test proves Integer.valueOf() compromise EA 
     * 
     * @author xinyi
     *
     */
    public static class TestAddInteger extends BenchmarkWorker {
        int val;
        
        @Override
        public void op() {
            Integer x = new Integer(hashCode());
            Integer y = new Integer(hashCode());
            Integer n = new Integer(x + y); // gc free
            //Integer n = x + y; // gc
            this.val = n;
        }
    }
    
    /**
     * local array can be escaped, no gc 
     * 
     * @author xinyi
     *
     */
    public static class TestLocalArray extends BenchmarkWorker {
        int val;
        
        @Override
        public void op() {
            int[] x = new int[] {hashCode()};
            int[] y = new int[] {hashCode()};
            int z = x[0] + y[0] + x.length + y.length;
            this.val = z;
        }
    }
    
    public static class TestReturnValue extends BenchmarkWorker {
        int val;
        
        @Override
        public void op() {
            Integer x = new Integer(hashCode());
            Integer y = new Integer(hashCode());
            Integer z = add(x,y);
            this.val = z;
        }
        
        Integer add(Integer x, Integer y) {
            //Integer z = new Integer(x + y); // gc free
            Integer z = x + y; // gc free only on 1.8
            return z;
        }
    }
    
    // gc free on java 1.8 but not on 1.7
    public static class TestBigDecimal extends BenchmarkWorker {
        long val;
        
        @Override
        public void op() {
            int hash = hashCode();
            BigDecimal x = new BigDecimal(hash);
            BigDecimal y = new BigDecimal(hash);
            this.val = add(x,y).longValue();
        }
        
        BigDecimal add(BigDecimal x, BigDecimal y) {
            BigDecimal z = x.add(y);
            return z;
        }
    }
    
    /**
     * string operation always cause GC
     * 
     * @author xinyi
     *
     */
    public static class TestString extends BenchmarkWorker {
        
        long val;
        ThreadLocal<StringBuilder> local = new ThreadLocal<>();
        
        @Override
        public void op() {
            StringBuilder buf = local.get();
            if (buf == null) {
                buf = new StringBuilder();
                local.set(buf);
            }
            buf.setLength(0);
            int z = buf.append("asdfa").append(System.lineSeparator()).length();
            //String zz = "asdfa".concat(System.lineSeparator());
            // int z = zz.length();
            this.val = z;
        }
        
        int add(String x, String y) {
            Integer xx = Integer.valueOf(x);
            Integer yy = Integer.valueOf(y);
            int z = xx + yy;
            return z;
        }
    }
    
    public static class SimpleSelect extends BenchmarkWorker {
        Connection conn = createConnection();
        PreparedStatement stmt;
        long hash;
        
        static {
            prepareData();
        }
        
        public SimpleSelect() throws SQLException {
            this.stmt = conn.prepareStatement("select *, customer_name || 'abc' from customer");
        }
        
        @Override
        public void op() {
            try {
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    Object col1 = rs.getObject(1);
                    Object col2 = rs.getObject(2);
                    Object col3 = rs.getObject(3);
                    this.hash = this.hash ^ col1.hashCode();
                    this.hash = this.hash ^ col2.hashCode();
                    this.hash = this.hash ^ col3.hashCode();
                }
                rs.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        
    }
    
    static Connection createConnection() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@hadoop:1521:orcl", "test", "test");
            return conn;
        }
        catch (Exception x) {
            x.printStackTrace();
            System.exit(-1);
            return null;
        }
    }
    
    static void prepareData() {
        try (Connection conn = createConnection()) {
            URL url = EscapeAnalysis.class.getResource("./escapeAnalysis.sql");
            String sql = IOUtils.toString(url);
            conn.createStatement().execute(sql);
        }
        catch (Exception x) {
            x.printStackTrace();
            System.exit(-1);
        }
        try (Connection conn = createConnection()) {
            URL url = EscapeAnalysis.class.getResource("./escapeAnalysis-2.sql");
            String sql = IOUtils.toString(url);
            conn.createStatement().execute(sql);
        }
        catch (Exception x) {
            x.printStackTrace();
            System.exit(-1);
        }
    }
    
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        // Benchmarker.run(5, 1, TestAddInteger.class);
        // Benchmarker.run(5, 1, TestLocalArray.class);
        Benchmarker.run(20, 1, SimpleSelect.class);
    }
}