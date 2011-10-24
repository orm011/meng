/**
 * 
 */
package com.twitter.dataservice.shardutils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


public class HierarchicalHashFunction implements IHashFunction {
  //by hierarchical we mean there are two levels to it (one per vertex)
  //basically the token for (edge(v1,v2)) is  (hash(v1),hash(v2))
  //this way, all edges for a given node are in a contiguous token range,
  //and I can use the same representation for all kinds of shards

   private static MessageDigest hashfun;

   static {
     try {
      hashfun = MessageDigest.getInstance("MD5");
     } catch (NoSuchAlgorithmException e) {
       throw new RuntimeException(e);
     }
   }

  public Token hash(Edge e) {
    byte[] left = hashfun.digest(e.getLeftEndpoint().toByteArray());
    byte[] right = hashfun.digest(e.getRightEndpoint().toByteArray());
    byte[] hashed = new byte[left.length + right.length];
    System.arraycopy(left, 0, hashed, 0, left.length);
    System.arraycopy(right, 0, hashed, left.length, right.length);

    return new Token(hashed);
  }

  public Pair<Token, Token> hash(Vertex v) {
    byte[] leftside = hashfun.digest(v.toByteArray());

    byte[] upperend = Arrays.copyOf(leftside, leftside.length*2);  
    Arrays.fill(upperend, leftside.length, upperend.length, (byte)0xff);
    
    byte[] lowerend = Arrays.copyOf (leftside, leftside.length*2);
    Arrays.fill(lowerend, leftside.length, lowerend.length, (byte)0);
 
    
    return new Pair<Token,Token>((new Token(lowerend)).predecessor(), new Token(upperend));
  }
  
  //TODO: remove references to old vertex hash.
  public Token hashVertex(Vertex v){
      return new Token(hashfun.digest(v.toByteArray()));
  }

  @Override
  public Token hash(Node n)
  {
      byte[] left = hashfun.digest(n.toByteArray());
      byte[] ans = Arrays.copyOf(left, 2*left.length);
      Arrays.fill(ans, left.length, ans.length, (byte)0xff);
      return new Token(ans);
  }

  @Override
  public List<Pair<Token, Node>> hashPartition(List<Node> nodes, Vertex v)
  {
      byte[] left = hashfun.digest(v.toByteArray());
      
      List<Pair<Token,Node>> results = new LinkedList<Pair<Token,Node>>();
      for (Node n: nodes){
          byte[] ringPos = Arrays.copyOf(left, 2*left.length);
          byte[] hash = hashfun.digest(n.toByteArray());
          System.arraycopy(hash, 0, ringPos, left.length, ringPos.length);          
          
          results.add(new Pair<Token, Node>(new Token(ringPos), n));
      }
            
      return results;      
  }
  
}