package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;

import com.google.common.primitives.Longs;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.remotes.IDataNode.NodeStats;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public interface IAPIServer
{
    public Edge getEdge(Vertex v, Vertex w);
    
    public List<Vertex> getFanout(Vertex v, int pageSize, int offset); 
    
    public List<Vertex> getIntersection(Vertex v, Vertex w, int pageSize, int offset);
    
    public void putEdge(Edge e);
    
    public void putFanout(int vertexid, int[] fanouts);
    
    public Collection<Stats> stat();
    
    abstract public class Stats{}
    
    public class SuccessfulStats extends Stats {
    	final long time;
    	final IDataNode.NodeStats nstats;
    	public static SuccessfulStats EMPTY;

    	static {
    		EMPTY = new SuccessfulStats(-1, NodeStats.EMPTY);
    	}
    	
    	public SuccessfulStats(long time, IDataNode.NodeStats nstats){
    		this.time = time;
    		this.nstats = nstats;
    	}
    	
    	@Override
    	public String toString(){
    		return String.format("node stats: %s stat() latency: %d", nstats, time);
    	}
    	
    	public SuccessfulStats merge(SuccessfulStats ss){
    		long time = Longs.max(this.time, ss.time);
    		return new SuccessfulStats(time, nstats.merge(ss.nstats));
    	}
    }
    
    public class FailedStats extends Stats {
    	RemoteException re;
    
    	public FailedStats(RemoteException re){
    		this.re = re;
    	}
    	
    	@Override
    	public String toString(){
    		return String.format("node stats: stat() failed for node. %s",  re.toString());
    	}
    }
}
