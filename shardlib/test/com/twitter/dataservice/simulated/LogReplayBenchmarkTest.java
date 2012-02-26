package com.twitter.dataservice.simulated;

import java.util.NoSuchElementException;

import org.junit.Test;
import com.twitter.dataservice.shardutils.Vertex;

import junit.framework.Assert;


public class LogReplayBenchmarkTest
{
    
    @Test
    public void checkEmpty(){
        LogReplayBenchmark lrb = new LogReplayBenchmark("test/com/twitter/dataservice/simulated/samplelog.log", 0);        
        
        //should not say I have next
        Assert.assertTrue(!lrb.hasNext());
        
        //should throw exception
        try {
            lrb.next();
            Assert.fail();
        } catch (NoSuchElementException nse){}
        
        //should remain empty
        Assert.assertTrue(!lrb.hasNext());
    }
    
    @Test
    public void checkIdempotentHasNext(){
        LogReplayBenchmark lrb = new LogReplayBenchmark("test/com/twitter/dataservice/simulated/samplelog.log", 1);        

        //should hold true repeatedly
        Assert.assertTrue(lrb.hasNext());
        Assert.assertTrue(lrb.hasNext());
        Assert.assertTrue(lrb.hasNext());

        lrb.next();
        Assert.assertTrue(!lrb.hasNext());
        Assert.assertTrue(!lrb.hasNext());
        
        try {
            lrb.next();
            Assert.fail();
        } catch (NoSuchElementException nse){}
    }
    
    @Test
    public void checkLinesRead(){
        LogReplayBenchmark lrb = new LogReplayBenchmark("test/com/twitter/dataservice/simulated/samplelog.log", Long.MAX_VALUE);
        
        int queries = 0;
        
        while (lrb.hasNext()){
            queries++;
            lrb.next();
        }
        
        //assert 
        Assert.assertEquals(queries, 10);
    }
    
    @Test
    public void checkOperationTypeParsing(){

        Query q1 = LogReplayBenchmark.parseQuery("2971 Edge 123322391 8897825");        
        Query q2 = LogReplayBenchmark.parseQuery("508 SimpleQuery_SimpleQuery_Intersection 4000,-1 145397885 145397885");
        Query q3 = LogReplayBenchmark.parseQuery("201 SimpleQuery 1000,-1 22782205");
        Query q4 = LogReplayBenchmark.parseQuery("112 SimpleQuery_SimpleQuery_Union 1,-1 172212633 172212633");
        Query q5 = LogReplayBenchmark.parseQuery("asdfasdfasdf");
        Query q6 = LogReplayBenchmark.parseQuery("");
                
        Assert.assertTrue(q1 instanceof Query.EdgeQuery);
        Assert.assertTrue(q2 instanceof Query.IntersectionQuery);
        Assert.assertTrue(q3 instanceof Query.FanoutQuery);
        Assert.assertTrue(q4 instanceof Query.NopQuery);
        Assert.assertTrue(q5 instanceof Query.NopQuery);
        Assert.assertTrue(q6 instanceof Query.NopQuery);   
    }
    
    @Test
    public void checkTabSeparated(){
        LogReplayBenchmark lrb = new LogReplayBenchmark("test/com/twitter/dataservice/simulated/testworkload.workload", 3);
        Query q = lrb.next();
        Assert.assertTrue(lrb.next() instanceof Query.FanoutQuery);
        Query.FanoutQuery r = (Query.FanoutQuery)q;
        Assert.assertEquals(new Vertex(17), ((Query.FanoutQuery)r).getVertex());
        Assert.assertEquals(29, ((Query.FanoutQuery)r).getPageSize());
        Assert.assertEquals(-1, ((Query.FanoutQuery)r).getOffset());
    }
    
    @Test
    public void checkEdgeQueryArgs(){
        Query.EdgeQuery q1 = (Query.EdgeQuery)LogReplayBenchmark.parseQuery("2971 Edge 123322391 8897825");
        Assert.assertEquals(new Vertex(123322391), q1.getLeftVertex());
        Assert.assertEquals(new Vertex(8897825), q1.getRightVertex());
    }
    
    int MAXIDALLOWED = 190000000;
    
    @Test
    public void checkFanoutQueryArgs(){
        Query.FanoutQuery q1 = (Query.FanoutQuery)LogReplayBenchmark.parseQuery("201 SimpleQuery 1000,-1 22782205");
        Assert.assertEquals(new Vertex(22782205), q1.getVertex());
        Assert.assertEquals(-1, q1.getOffset());
        Assert.assertEquals(1000, q1.getPageSize());
        
        Query.FanoutQuery q2 = (Query.FanoutQuery)LogReplayBenchmark.parseQuery("201 SimpleQuery 1000,-12343243434 22782205");
        Assert.assertEquals(new Vertex(22782205), q2.getVertex());
        Assert.assertEquals(1000, q2.getPageSize());
        Assert.assertTrue(q2.getOffset() >= 0 && q2.getOffset() < MAXIDALLOWED);
    }
    
    @Test
    public void checkIntersectionQueryArgs(){
        Query.IntersectionQuery q1 = (Query.IntersectionQuery)
        LogReplayBenchmark.parseQuery("11214234 SimpleQuery_SimpleQuery_Intersection 1,-1 172212633 172212633");    
        
        Assert.assertEquals(new Vertex(172212633), q1.getLeftVertex());
        Assert.assertEquals(new Vertex(172212633), q1.getRightVertex());
        Assert.assertEquals(-1, q1.getOffset());
        Assert.assertEquals(1, q1.getPageSize());

        Query.IntersectionQuery q2 = (Query.IntersectionQuery)
        LogReplayBenchmark.parseQuery("112 SimpleQuery_SimpleQuery_Intersection 40,12343125543 1722126333 172212633");
        Assert.assertEquals(new Vertex(1722126333), q2.getLeftVertex());
        Assert.assertEquals(new Vertex(172212633), q2.getRightVertex());
        Assert.assertTrue(q2.getOffset() >= 0 && q2.getOffset() < MAXIDALLOWED);
        
        Assert.assertEquals(40, q2.getPageSize());
    }
}
