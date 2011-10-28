package com.twitter.dataservice.simulated;

import org.apache.commons.math.distribution.ZipfDistributionImpl;
import org.junit.Test;


public class BenchmarkTest
{    
    
    //this collects a bunch of numbers, which I can plot and check what the
    //distribution look like. (Not a real test)
    @Test 
    public void numberGeneration(){
        ZipfDistributionImpl myimp = new ZipfDistributionImpl(2,1);
        ZipfDistributionImpl myimp2 = new ZipfDistributionImpl(1,1);
        for (int i = 0; i < 50; i++){
            System.out.println(myimp.probability(i));
        }
        
        System.out.println("-----------");
        for (int i = 0; i < 50; ++i){
            System.out.println(myimp2.probability(i));
        }
    }
}
