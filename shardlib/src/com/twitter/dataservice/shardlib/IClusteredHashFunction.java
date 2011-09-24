package com.twitter.dataservice.shardlib;

public interface IClusteredHashFunction extends IHashFunction
{        
        public Pair<IToken, IToken> apply(Vertex v);
}
