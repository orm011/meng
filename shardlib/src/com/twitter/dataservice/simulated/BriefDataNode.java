package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.Arrays;

import com.google.common.primitives.Ints;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Vertex;

public class BriefDataNode extends CompactDataNode implements IDataNode {
	
	int work;
	
	public BriefDataNode(int numVertices, String name, int work){
		super(numVertices, name);
		this.work = work;
	}
	
	 @Override
	 public int[] getFanout(Vertex v, int pageSize, int offset) throws RemoteException
	 {     
		 super.log.debug("getFanout from BriefDataNode");
		 int[] answer = super.getFanout(v, pageSize, offset);
		 
		 //adding some extra work proportional to size.
		 long total = 0;
		 for (int i = 0; i < work; i++){
			 for (int pos = 0; pos < answer.length; pos++){
				 total += answer[pos];
			 }
		 }

		 int MAXSIZE = 1;
		 int[] ans = Arrays.copyOf(answer, Ints.min(answer.length, MAXSIZE)); 
		 int[] count = new int[]{(int)total};
		 return Ints.concat(count, ans);
	 }
	 
	 
}
