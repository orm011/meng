package com.twitter.dataservice.shardutils;

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
    
    @Override public boolean equals(Object o){
        if (o != null && (o instanceof Pair<?,?>)){
            Pair<?,?> p = (Pair<?,?>)o;
            return p.getLeft().equals(this.getLeft()) && p.getRight().equals(this.getRight());
        }
        
        return false;       
    }
    
    @Override public int hashCode(){
        return (getLeft().hashCode() & 0xFFFF0000) + (getRight().hashCode() & 0xFFFF);
    }
    
    @Override public String toString(){
        return "Pair: Left: " + getLeft().toString() + " Right: " + getRight().toString();
    }
}