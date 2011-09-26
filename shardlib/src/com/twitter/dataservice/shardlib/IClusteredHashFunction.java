package com.twitter.dataservice.shardlib;

public interface IClusteredHashFunction extends IHashFunction
{        
        public Pair<Token, Token> apply(Vertex v);
}
