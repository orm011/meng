package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.commons.lang.NotImplementedException;

import com.google.common.primitives.Ints;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;


/*
 * TODO: how do we make sure there are enough buckets in the hashtable?
 */
public class CompactDataNode extends AbstractDataNode implements IDataNode
{
    Logger log = LoggerFactory.getLogger(CompactDataNode.class);
    //consistency condition: there is an edge (u, v) for every v in fanout(u)
    //note the edge object also contains the pair.    
    private HashMap<Integer, int[]> fanouts;
    private final String name;
    
    
    public CompactDataNode(int numVertices, String name){       
        fanouts = new HashMap<Integer, int[]>(numVertices);    	
        this.name = name;
    }
    
    public CompactDataNode(int numVertices){
        this(numVertices, "defaultName");
    }
    
    public CompactDataNode(){
        fanouts = new HashMap<Integer, int[]>();
        name = "defaultName";
    }
    
    @Override
    public Edge getEdge(Vertex left, Vertex right) throws RemoteException
    {
        log.debug(String.format("getEdge: %s %s\n", left, right));
        int[] fans = fanouts.get(left.getId());
        
        if (fans == null) fans = new int[0];
        
        
        int index;
        if ((index = Arrays.binarySearch(fans, right.getId())) >= 0){
            return new Edge(left.getId(), fans[index]);
        } else {
            throw new AssertionError("missing edge");
        }
    }

    @Override
    public int[] getFanout(Vertex v, int pageSize, int offset) throws RemoteException
    {     
        log.debug("getFanout: {}, {}, {}", new Object[]{v, pageSize, offset});;        
        int[] fullfanout = fanouts.get(v.getId());
        
        if (fullfanout == null)  throw new RemoteException(v.toString() + " not found");
        
        int pos = UtilMethods.getInsertionIndex(fullfanout, offset);
        int[] result = Arrays.copyOfRange(fullfanout, pos, Ints.min(new int[]{pos + pageSize, fullfanout.length}));
        
        log.debug("fanout answer. size: {}", result.length);
        return result;
    }

    @Override
    public int[] getIntersection(Vertex v, Vertex w, int pageSize, int offset) throws RemoteException
    {
        log.debug("getIntersection {}, {}, {}, {}", new Object[]{v, w, pageSize, offset});
        int[] vfanout = fanouts.get(v.getId());
        int[] wfanout = fanouts.get(w.getId());
        
        String vertices = (vfanout == null? v.toString():"") + " " + (wfanout == null? w.toString():"");
        if (vfanout == null || wfanout == null) throw new RemoteException(vertices + " not found");
        
        return UtilMethods.intersectSortedUniqueArraySet(vfanout, wfanout, pageSize, offset);
    }

    public void putFanout(int id, int[] fanouts){
        log.debug("remote put fanout: {}", id);
        this.fanouts.put(id, fanouts);
    }
    
    public void localPutFanout(int id, int[] fanouts){
        log.debug("local put fanout: {}", id);
        this.fanouts.put(id, fanouts);
    }
    
    
    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        log.warn("call to putEdge operation. Ignoring.");
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() throws RemoteException
    {
        log.warn("RESETTING...");
        fanouts.clear();
    }

    @Override
    public int totalLoad() throws RemoteException
    {
    	throw new UnsupportedOperationException();
    }
    
    @Override
	public IDataNode.NodeStats stat() throws RemoteException
    {
    	int totalV = 0;
    	int totalD = 0;
    	int maxD = -1;
    	
    	for (int[] fanout : fanouts.values()){
    		totalV += 1;
    		totalD += fanout.length;
    		maxD = Ints.max(maxD, fanout.length);
    	}
    	
    	return new IDataNode.NodeStats(this.name, totalV, totalD, maxD);
    }
    
    public void finishLoading() {
        throw new NotImplementedException();
    }
    
}