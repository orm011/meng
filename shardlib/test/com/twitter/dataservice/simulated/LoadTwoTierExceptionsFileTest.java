package com.twitter.dataservice.simulated;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;


public class LoadTwoTierExceptionsFileTest
{
    @Test
    public void testLoadFile(){
        int[] ans = Benchmark.loadExceptionFile("/Users/oscarm/workspace/oscarmeng/shardlib/test/com/twitter/dataservice/simulated/testTwoTierExceptionfile.uid");
        Assert.assertTrue(Arrays.equals(ans, new int[]{1,2}));
    }
}
