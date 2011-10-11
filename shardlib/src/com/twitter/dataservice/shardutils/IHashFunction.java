package com.twitter.dataservice.shardutils;

import java.util.List;

public interface IHashFunction
{
    //gives you a single token
    public Token hash(Edge e);
    
    //a vertex gives you two endpoints of a token range
    public Pair<Token, Token> hash(Vertex v);
    
    //hashing nodes
    public Token hash(Node n);

    //partitioning
    public List<Pair<Token,Node>> hashPartition(List<Node> nodes, Vertex v);
}
