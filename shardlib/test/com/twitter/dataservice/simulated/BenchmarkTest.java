package com.twitter.dataservice.simulated;

import org.junit.Test;


public class BenchmarkTest
{    
    
    //this collects a bunch of numbers, which I can plot and check what the
    //distribution look like. (Not a real test)
    @Test 
    public void numberGeneration(){
        for (int i = 0; i < 1000; i++){
            System.out.println(Benchmark.nextInt());
        }
    }
}
