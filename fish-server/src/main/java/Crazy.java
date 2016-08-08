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


public class Crazy {

    public static void main(String[] args) {
        new Crazy().run();
    }

    void run() {
        // init
        
        long count = 1000000000l;
        
        // start test 1
        
        long start = System.currentTimeMillis();
        
        long sum = 0;
        for (long i=0; i<count; i++) {
            sum = add(sum);
        }
        
        long end = System.currentTimeMillis();
        System.out.println(end - start);

        // start test 2
        
        long start2 = end;
        sum = 0;
        for (long i=0; i<count; i++) {
            sum = add1(sum);
        }
        
        long end2 = System.currentTimeMillis();
        System.out.println(end2 - start2);
}
    
    long add(long sum) {
        int hash = hashCode();
        BigDecimal number = new BigDecimal(hash);
        sum += number.longValue();
        return sum;
    }

    long add1(long sum) {
        BigDecimal number = new BigDecimal(hashCode());
        sum += number.longValue();
        return sum;
    }
}
