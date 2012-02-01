package com.twitter.dataservice.simulated;

import com.twitter.dataservice.shardutils.Vertex;

public class MissingVertexException extends Exception
{
    Vertex v;

    public MissingVertexException(Vertex v){
        this.v = v;
    }
    
}
