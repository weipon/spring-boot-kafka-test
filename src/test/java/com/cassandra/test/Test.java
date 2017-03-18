package com.cassandra.test;



import java.util.HashSet;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by weipon on 17/3/14.
 */
public class Test {

    public static void main(String[] args) {
         LinkedBlockingDeque<Long> shopIdsQueue=new LinkedBlockingDeque<Long>(50000);
        shopIdsQueue.add(1000l);
        shopIdsQueue.add(1000l);
        HashSet<Long> stockList=new HashSet<Long>();
        if (shopIdsQueue.size()==0)
            return;
        shopIdsQueue.drainTo(stockList);

        System.out.println(stockList);



    }

}
