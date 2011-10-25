package com.twitter.dataservice.simulated;

import org.junit.Test;


public class BenchmarkTest
{       
    @Test 
    public void numberGeneration(){
        for (int i = 0; i < 1000; i++){
            System.out.println(Benchmark.nextInt());
        }
    }
}
