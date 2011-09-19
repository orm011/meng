package com.twitter.dataservice.shardlib;

public interface IHashFunction
{
    public IToken apply(Edge e);
}
