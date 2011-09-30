package com.twitter.dataservice.shardutils;

public interface IHashFunction
{
    //gives you a single token
    public Token hash(Edge e);
    
    //a vertex gives you two endpoints of a token range
    public Pair<Token, Token> hash(Vertex v);
}
