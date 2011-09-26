package com.twitter.dataservice.shardlib;

public class Pair<L,R>
{
    private L left;
    private R right;
    
    public Pair(L leftmem, R rightmem){
        this.left = leftmem;
        this.right = rightmem;
    }
    
    public L getLeft(){
        return left;
    }
    public R getRight(){
        return right;
    }
}
