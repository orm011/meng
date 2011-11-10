package com.twitter.dataservice.sharding;

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

import com.twitter.dataservice.shardutils.*;

//most basic sharding. edge goes to  a Token.
public class TwoTierHashSharding implements ISharding
{

  public int getNumShards(){
      return shards.size();
  }
  
  //TODO: move these things, and system parameters into single location read from external file.
  private final static int DEFAULT_NUMSHARDS = 1000;
  private final static int DEFAULT_EXCEPTION_TIER_SPLIT = 2;
  private final static int DEFAULT_EXCEPTION_TIER_NUM_NODES = 2;
  
  SortedMap<Token,Node> shards = new TreeMap<Token, Node>();

  final static HierarchicalHashFunction hashfun = new HierarchicalHashFunction();

  IShardPrimitives state; //not sure about this reference.
  
  
  //Methods to keep state up to date across all shardlibs. (todo later)
  //sets up the local state based the given zkState (initialization)
  public TwoTierHashSharding(IShardPrimitives zkState){
      //
  }
  
  public TwoTierHashSharding(List<Vertex> exceptions, List<Node> nodes, int numShards, int numShardsPerException, int numNodesPerException){
      List<Token> commonShards = Token.splitFullTokenSpace(Token.DEFAULT_PREFIX_LENGTH, numShards);
      CycleIterator<Node> nodeIt = new CycleIterator<Node>(nodes, nodes.iterator());
      
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
          List<Token> vertexShards = Token.splitNodeImpliedRange(prefix, TwoTierHashSharding.DEFAULT_EXCEPTION_TIER_SPLIT);
          
          shards.put(predecessor, start);
          for (Token tk: vertexShards){
               shards.put(tk, localRoundRobin.next());
          }          
      }   
  }
  
  
  //for test purposes TODO: remove this/make private, somehow
  public TwoTierHashSharding(List<Pair<Token, Node>> shardState){
      if (shardState.size() == 0)
          throw new IllegalArgumentException("system must have at least one shard");
      
      for (Pair<Token, Node> pair : shardState){
          shards.put(pair.getLeft(), pair.getRight());
      }
  }
  
  public void applyStateUpdate(IShardUpdate zkUpdate){
      //applies local state based on a delivered update
  }
  
  //client inits the remote zkState (do we do this any differently from a state update?)
  public void initRemoteHashSharding(){
      //takes snapshot of local state and sends it
  }
  
  public void publishStateUpdate(){
      //sends update to zk (assumes you have applied it/waits for it to come back?/applies it here?)
  }
  
  //Methods to read the shard state  
  @Override public Pair<Shard,Collection<Node>> getShardForEdgeQuery(Edge e) {
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

 //TODO: define thresholds with some basis
  private final static int PROMOTE_NODE = 200000;
  private final static int TOLERANCE = 2000;
  private final static int DEMOTE_NODE = PROMOTE_NODE - TOLERANCE;
  private final static int REPLICATION_FACTOR = 3;
 
  //also temporary
  private final static int SHARDS_PER_SPECIAL_NODE = 4;
  //private final static int OPTIMAL_SINGLE_NODE_SHARD_SIZE = PROMOTE_NODE;
  
  private void demoteVertex(Vertex v){
      //update clutch: remove assignments
      //wait on clutch state update
      //update local: remove shards from list. 
      //don't need this for static version
  }
  
  
  private void promoteVertex(Vertex v){
      //TODO: later
      //from clutch update if possible
      //wait on clutch assignment states update
      //update local to reflect those changes    
  }
  
  private void internalPromoteVertex(Vertex v){
      //option 1:
      // List<Token> parts = Token.split(pt.getLeft(), pt.getRight(), 2);      
      //TODO: need to figure out way of promting in a way that
      //keeps balance, so would like to know who else is in charge of a special node.
      //TODO: shards.put(tk, ??)  how to make it balanced? how to make it alright when new node added?
      
      //option 2: pick nodes, hash, use pair (hash(node_i), nodei)
  }
  
}