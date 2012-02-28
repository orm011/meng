package com.twitter.dataservice.remotes;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public interface IDataNode extends Remote
{
    Edge getEdge(Vertex left, Vertex right) throws  RemoteException;
    
    int[] getFanout(Vertex v, int pageSize, int offset) throws RemoteException;

    Collection<Vertex> getFanout(Vertex v) throws RemoteException;
    
    int[] getIntersection(Vertex v, Vertex w, int pageSize, int offset) throws RemoteException;
    
    void putFanout(int vertex, int[] fanout) throws RemoteException;
    
    void putEdge(Edge e) throws RemoteException;
    
    //Random walk. Need to get to this later. <- haha forget it bro.    
    //some utilities
    void reset() throws RemoteException;
    
    int totalLoad() throws RemoteException;
        
    NodeStats stat() throws RemoteException;
    

    
    public class NodeStats {
    	@Override
		public String toString() {
			return "NodeStats [maxDegree=" + maxDegree + ", name=" + name
					+ ", totalDegree=" + totalDegree + ", totalVertices="
					+ totalVertices + "]";
		}

		final String name;
    	final long totalVertices;
    	final long totalDegree;
    	final long maxDegree;
    	
    	public static final NodeStats EMPTY;
    	static{
    		EMPTY = new NodeStats("", 0, 0, -1);
    	}
    	
    	public NodeStats(String nodeName, long totalV, long totalD, long maxD){
    		name = nodeName;
    		totalVertices = totalV;
    		totalDegree = totalD;
    		maxDegree = maxD;
    	}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (maxDegree ^ (maxDegree >>> 32));
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result
					+ (int) (totalDegree ^ (totalDegree >>> 32));
			result = prime * result
					+ (int) (totalVertices ^ (totalVertices >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof NodeStats))
				return false;
			NodeStats other = (NodeStats) obj;
			if (maxDegree != other.maxDegree)
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (totalDegree != other.totalDegree)
				return false;
			if (totalVertices != other.totalVertices)
				return false;
			return true;
		}
    	
		//TODO: test this?
		public NodeStats merge(NodeStats ns){
			String name = this.name + ":" + ns.name;
			long maxDegree = Longs.max(this.maxDegree, ns.maxDegree);
			long totalD = this.totalDegree + ns.totalDegree;
			long totalV = this.totalVertices + ns.totalVertices;
			
			return new NodeStats(name, totalV, totalD, maxDegree);
		}
		
    }
}