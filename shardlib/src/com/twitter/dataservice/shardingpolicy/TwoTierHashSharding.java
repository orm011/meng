package com.twitter.dataservice.shardingpolicy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.sharding.CycleIterator;
import com.twitter.dataservice.sharding.IShardPrimitives;
import com.twitter.dataservice.sharding.ISharding;
import com.twitter.dataservice.sharding.SlidingWindowCycleIterator;
import com.twitter.dataservice.shardutils.*;

//most basic sharding. edge goes to  a Token.
public class TwoTierHashSharding implements ISharding
{

  public int getNumShards(){
      return shards.size();
  }
    
  SortedMap<Token,Node> shards = new TreeMap<Token, Node>();

  final static HierarchicalHashFunction hashfun = new HierarchicalHashFunction();

  IShardPrimitives state; //not sure about this reference.
  
  //NOTE: only meant to be used right now to test the parallelism of the system
  //this basically assumes nodes 0 - numExceptions -1 are exceptions, makes sense if these are sorted or by #edges.
  static public TwoTierHashSharding makeTwoTierHashFromNumExceptions(int numExceptions, Map<Node, IDataNode> nodes, int numOrdinaryShards, int numShardsPerException, int numNodesPerException){
      List<Vertex> exceptions = new ArrayList<Vertex>(numExceptions);
      for (int i = 0; i < numExceptions; i++){
          exceptions.add(new Vertex(i));
      }
      
      return new TwoTierHashSharding(exceptions, nodes, numOrdinaryShards,numShardsPerException, numNodesPerException);      
  }

  public TwoTierHashSharding(List<Vertex> exceptions, Map<Node, IDataNode> nodes, int numOrdinaryShards, int numShardsPerException, int numNodesPerException){
      List<Node> keys = new ArrayList<Node>(nodes.keySet());
      List<Token> commonShards = Token.splitFullTokenSpace(Token.DEFAULT_PREFIX_LENGTH, numOrdinaryShards);
      CycleIterator<Node> nodeIt = new CycleIterator<Node>(keys, keys.iterator());
      
      //put all common shards, assign nodes to them in round robin way
      for (Token tk: commonShards){
          shards.put(tk, nodeIt.next());
      }
      
      //for each heavy vertex, DO THE RIGHT THING: 
      //insert a 0th shard to take care of the previous node's edges.
      //then insert shards partitioning its range
      for (Vertex v: exceptions){          
          Node start = nodeIt.next();
          //pick which nodes to cycle through and get iterator for them
          List<Node> nodesForVertex = new ArrayList<Node>(numNodesPerException);
          for (int i = 0; i < numNodesPerException; i++){
              nodesForVertex.add(nodeIt.next());
          }
          
          CycleIterator<Node> localRoundRobin = new CycleIterator<Node>(nodesForVertex, nodesForVertex.iterator());
          
          Token prefix = hashfun.hashVertex(v);
          Pair<Token,Token> ends = hashfun.hash(v);
          Token predecessor = ends.getLeft();
          List<Token> vertexShards = Token.splitNodeImpliedRange(prefix, numShardsPerException);
          
          shards.put(predecessor, start);
          for (Token tk: vertexShards){
               shards.put(tk, localRoundRobin.next());
          }          
      }   
  }
  
  
  //@VisibleForTesting
  public TwoTierHashSharding(List<Pair<Token, Node>> shardState){
      if (shardState.size() == 0)
          throw new IllegalArgumentException("system must have at least one shard");
      
      for (Pair<Token, Node> pair : shardState){
          shards.put(pair.getLeft(), pair.getRight());
      }
  }
  
  
  //Methods to read the shard state  
  @Override public Pair<Shard,Collection<Node>> getShardForEdgeQuery(Edge  e) {
        Token tok = hashfun.hash(e);
        List<Pair<Token, List<Node>>> queryResult = hashRingRangeQuery(new Pair<Token, Token>(tok, tok));
        
        assert queryResult.size() == 1;
        Pair<Token, List<Node>> result = queryResult.get(0);
        
        return new Pair<Shard, Collection<Node>>(new Shard(result.getLeft()), result.getRight());
  }
  
  public List<Pair<Shard,Collection<Node>>> getShardForVertexQuery(Vertex v) {
      List<Pair<Shard,Collection<Node>>> answer = new LinkedList<Pair<Shard,Collection<Node>>>();
      Pair<Token,Token> ends = hashfun.hash(v);

      List<Pair<Token, List<Node>>> queryResult = hashRingRangeQuery(ends);
      for (Pair<Token, List<Node>> pr : queryResult){
          answer.add(new Pair<Shard, Collection<Node>>(new Shard(pr.getLeft()), pr.getRight())); 
      }
      
      return answer;
  }

  //gets a view of the shard-tokens within the input range, 
  //with a corresponding *meaningfully sorted* set of replicas
  //the left end of the tokenRange should be smaller than the rightEnd
  public List<Pair<Token, List<Node>>> hashRingRangeQuery(Pair<Token, Token> tokenRange){
      Map<Token,Set<Node>> prelim = new LinkedHashMap<Token,Set<Node>>();
      
      SortedMap<Token, Node> tail = shards.tailMap(tokenRange.getLeft());
 
      CycleIterator<Map.Entry<Token,Node>> it = new CycleIterator<Map.Entry<Token,Node>>(shards.entrySet(), 
              tail.entrySet().iterator());
  
      CycleIterator<Map.Entry<Token,Node>> ot = new CycleIterator<Map.Entry<Token,Node>>(shards.entrySet(), 
              tail.entrySet().iterator());
      
      SlidingWindowCycleIterator<Map.Entry<Token,Node>> window = new SlidingWindowCycleIterator<Map.Entry<Token,Node>>(ot, 
              TwoTierHashSharding.REPLICATION_FACTOR);
  
      {
          Map.Entry<Token, Node> ent = null;
          
          if (!it.hasNext()) {
              throw new IllegalArgumentException("should have at least one shard");
          }
          
          do {
           ent = it.next();

           Set<Node> nodes = new LinkedHashSet<Node>();
           
           List<Map.Entry<Token, Node>> entries = window.next();
           for (Map.Entry<Token, Node> mape: entries){
               nodes.add(mape.getValue());
           }
           
           prelim.put(ent.getKey(), nodes);
          } while(ent.getKey().compareTo(tokenRange.getRight()) < 0 && !it.wrappedAround());          
      }
      
      List<Pair<Token, List<Node>>> toReturn = new LinkedList<Pair<Token, List<Node>>>();
      for (Map.Entry<Token, Set<Node>> mentry: prelim.entrySet()){
          toReturn.add(new Pair<Token, List<Node>>(mentry.getKey(), new LinkedList<Node>(mentry.getValue())));
      }
      
      return toReturn;
  }
  
  @Override public Collection<Edge> getEdgesWithinShard(Shard sh, Collection<Edge> edges) {
        //to be executed at data nodes.
      // for this ISharding method, it would work by looking at token ranges.
      return null;
  }

  private final static int REPLICATION_FACTOR = 3; 
  /*
   * really, probably not useful at this point,
   * but since it is being used elsewhere (tests etc)
   */
}