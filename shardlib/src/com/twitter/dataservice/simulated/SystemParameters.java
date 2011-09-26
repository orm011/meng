package com.twitter.dataservice.simulated;

public class SystemParameters
{
    //size in bytes for an edge
    public static double edgeMetadataSize = 100;
    
    //cost of reading an edge in nanos
    public static double edgecost = 250000*edgeMetadataSize/(10^6);
    
    public static final int workersPerNode = 6;

    public static final int numberOfWorkNodes = 4;
}
