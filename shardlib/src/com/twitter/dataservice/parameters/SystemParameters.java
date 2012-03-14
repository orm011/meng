package com.twitter.dataservice.parameters;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


//parameters that are not meant to be changed in as we change workloads, 
//but which may need to be changed before settling on a final value.
public abstract class SystemParameters extends AbstractParameters
{    
    public static final String NUM_DATA_NODES = "system.numDataNodes";
    public static final String PER_EDGE_WEIGHT = "system.perEdgeWeight";
}
