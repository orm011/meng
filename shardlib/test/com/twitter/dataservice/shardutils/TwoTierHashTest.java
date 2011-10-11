package com.twitter.dataservice.shardutils;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.twitter.dataservice.sharding.TwoTierHashSharding;

public class TwoTierHashTest
{
    List<Pair<Token, Node>> state;
    TwoTierHashSharding tths = null;
    SortedSet<Token> tokenset = null;
    
    byte[][] bytez = {
            new byte[]{(byte) 0x00, (byte) 0xff},
            new byte[]{(byte) 0x0f, (byte) 0xff},
            new byte[]{(byte) 0xef, (byte) 0xff},
    };
    
    
    
    @Before
    public void prepareShardState(){
        state = new LinkedList<Pair<Token, Node>>();
        
        for (int  i = 0; i < bytez.length; i++){
            Pair<Token, Node> first = new Pair<Token, Node>(new Token(bytez[i]), new Node(i));
            state.add(first);
        }
        
        tths = new TwoTierHashSharding(state);    

        tokenset = new TreeSet<Token>();
        for (Pair<Token, Node> p: state)
            tokenset.add(p.getLeft());
        
    }
    
    
    
    @Test
    public void cycleIteratorTest(){
        
        //try a single element partition
        SortedSet<Token> subset = tokenset.subSet(new Token(bytez[0]), new Token(bytez[1]));
        TwoTierHashSharding.CycleIterator<Token> single = new TwoTierHashSharding.CycleIterator<Token>(subset);
        
        Assert.assertTrue(!single.wrappedAround());
        for (int i = 0; i < 5; i++){
            Assert.assertTrue(single.hasNext());
            Assert.assertTrue(single.next().equals(new Token(bytez[0])));
            Assert.assertTrue(single.wrappedAround());
        }
        
        //several elements
        //initialize iterator to offset
        Iterator<Token> usedIter = tokenset.iterator();
        usedIter.next();
        
        TwoTierHashSharding.CycleIterator<Token> at = new TwoTierHashSharding.CycleIterator<Token>(tokenset, tokenset.iterator());
        TwoTierHashSharding.CycleIterator<Token> et = new TwoTierHashSharding.CycleIterator<Token>(tokenset);
        TwoTierHashSharding.CycleIterator<Token> it = new TwoTierHashSharding.CycleIterator<Token>(tokenset, usedIter);
        Assert.assertTrue(at.hasNext() && et.hasNext() && it.hasNext());

        //iterate three times check it wraps around contents, 
        //check the wrapped flag is set at the right times only.
        for (int i = 0; i < 3*bytez.length; i++){
            Assert.assertTrue(at.wrappedAround() == ((i % bytez.length) == 0 && i != 0));
            Assert.assertTrue(at.next().equals(new Token(bytez[i % bytez.length])));
            Assert.assertTrue(et.next().equals(new Token(bytez[i % bytez.length])));
            Assert.assertTrue(at.wrappedAround() == et.wrappedAround());
            Assert.assertTrue(it.wrappedAround() == (((i + 1) % bytez.length) == 0));
            Assert.assertTrue(it.next().equals(new Token(bytez[(i + 1) % bytez.length])));
        }

    }
    
    @Test
    public void windowIteratorTest(){
        TwoTierHashSharding.CycleIterator<Token> cycle1 = new TwoTierHashSharding.CycleIterator<Token>(tokenset);
        TwoTierHashSharding.CycleIterator<Token> cycle2 = new TwoTierHashSharding.CycleIterator<Token>(tokenset);
        TwoTierHashSharding.CycleIterator<Token> cycle3 = new TwoTierHashSharding.CycleIterator<Token>(tokenset);
        
        Iterator<List<Token>> windowcycle1 = new TwoTierHashSharding.SlidingWindowCycleIterator<Token>(cycle1, 1);       
        Iterator<List<Token>> windowcycle2 = new TwoTierHashSharding.SlidingWindowCycleIterator<Token>(cycle2, 2); 
        Iterator<List<Token>> windowcycle4 = new TwoTierHashSharding.SlidingWindowCycleIterator<Token>(cycle3, 4);
        
        Assert.assertTrue(windowcycle1.hasNext() && windowcycle2.hasNext() && windowcycle4.hasNext());

        List<Token> next1;
        List<Token> next2;
        List<Token> next4;
 
        for (int i = 0; i < 3*bytez.length; i++){
            next1 = windowcycle1.next();
            Assert.assertEquals(1, next1.size());

            next2 = windowcycle2.next();
            Assert.assertEquals(2, next2.size());
            
            next4 = windowcycle4.next();
            Assert.assertEquals(4, next4.size());
            
            //test the results lists have the correct values, including ordering            
            Assert.assertEquals(new Token(bytez[i % bytez.length]), next1.get(0));
            Assert.assertEquals(next2.get(0), next1.get(0));
            Assert.assertEquals(new Token(bytez[(i+1) % bytez.length]), next2.get(1));
            Assert.assertEquals(next2.get(0), next4.get(0));
            Assert.assertEquals(next4.get(1), next2.get(1));
            Assert.assertEquals(new Token(bytez[(i+2) % bytez.length]), next4.get(2));
            Assert.assertEquals(new Token(bytez[(i+3) % bytez.length]), next4.get(3));
        }
        
    }
    
    private TwoTierHashSharding makeSharding(List<Token> tokens){
        int i = 0;
        
        List<Pair<Token, Node>> in = new LinkedList<Pair<Token,Node>>();
        for (Token tk: tokens){
            Pair<Token, Node> pr = new Pair<Token, Node>(tk, new Node(i));            
            i++;
            in.add(pr);
        }
        
        return new TwoTierHashSharding(in);        
    }
    
    @Test
    public void hashRingRangeQueryTest(){
        
        Token[] testShardSet = new Token[]{
                new Token(new byte[]{(byte)0x10}),
                new Token(new byte[]{(byte)0x08}),
                new Token(new byte[]{(byte)0x18}),
        };        
        List<Token> testShardList = Arrays.asList(testShardSet);

    
        Token[] testTokens = new Token[]{
                new Token(new byte[]{(byte) 0x01}),
                new Token(new byte[]{(byte) 0x10}), 
                new Token(new byte[]{(byte) 0x11}),
                new Token(new byte[]{(byte) 0xff}),
        };
        
        //test with only one shard. 
        TwoTierHashSharding hashRing1= makeSharding(testShardList.subList(0, 1));
        Pair<Token, Token> testPair1 = new Pair<Token, Token>(testTokens[0], testTokens[0]);
        List<Pair<Token, Collection<Node>>> result1 = hashRing1.hashRingRangeQuery(testPair1);
        Assert.assertEquals(1, result1.size());
        Assert.assertEquals(testShardSet[0], result1.get(0).getLeft());
        
        //with a wraparound
        Pair<Token, Token> testPair2 = new Pair<Token,Token>(new Token(new byte[]{(byte) 0xff}), new Token(new byte[]{(byte) 0xff}));
        List<Pair<Token, Collection<Node>>> result2 = hashRing1.hashRingRangeQuery(testPair2);
        Assert.assertEquals(1, result2.size());
        Assert.assertEquals(testShardSet[0], result1.get(0).getLeft());
        
    
        //with a wraparound, but more shards 
        TwoTierHashSharding hashRing2 = makeSharding(testShardList.subList(0, 2));
        List<Pair<Token, Collection<Node>>> result3 = hashRing2.hashRingRangeQuery(testPair2);
        //Assert.assertEquals(1, result3.size());
        System.out.println(result3.get(0).getLeft());
        System.out.println(result3.get(1).getLeft());
        Assert.assertEquals(testShardSet[1], result3.get(0).getLeft());
    }
    
//
//    @Test
//    public void testShardingAfterNodeAddress()
//    {
//        Vertex v = new Vertex(1);
//        TwoTierHashSharding.HierarchicalHashFunction hashfun = new TwoTierHashSharding.HierarchicalHashFunction();        
//        Pair<Token,Token> pt = hashfun.hash(v); 
//        
//        List<Pair<Shard, Collection<Node>>> answer = tths.getShardForVertexQuery(v);
//        
//        //must have exactly one node for this example
//        Assert.assertTrue(answer.size()  ==  1);
//        for (Pair<Shard, Collection<Node>> p: answer)
//            Assert.assertTrue(p.getLeft().getLowerEnd().compareTo(pt.getLeft()) >= 0 
//                        || p.getLeft().getLowerEnd().equals(new Token(bytez)));        
//    }
//    

}