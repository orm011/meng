package com.twitter.dataservice.simulated;

public class WorkTask implements Runnable
{
    //measures the amount of tuples to read
    private int workAmount;
    
    //measures the work necessary to read a single edge
    public WorkTask(int workAmount){
        this.workAmount = workAmount;
    }
    @Override
    public void run()
    {
        //TODO: specify strategy to generate the workamount parameter on every query.
        //idea: API has graph edge distribution stored, samples from it to get degree amount
        //then degree + sharding strategy get combined to produce an estimate sent to each machine.
        // eg:
        //  degree_gen.next() = 10million
        //  loadfactor = twoTierStrategy.getPerNodeWorkFactor(10million) = 200k(max work per node) 
        //  twoTierStrategy.getShards(key)  = {1,2,3,4}
        // servers.get(1).read(loadFactor);
        
        //TODO: sleeping seems to create good latency characteristics for this thread,
        // but I am not sure if it has the effect on shared resources needed to model
        // interactions with other threads.  Ie. If a thread is reading from memory, 
        // can other threads also read from memory? int hat case there is a contention cost
        // not reflected here (presumably many threads can sleep at the same time, but not read at the same time)
        try
        {
            Thread.sleep(100);
            System.out.println("work done!");
        } catch (InterruptedException e)
        {
            // TODO log these things
            e.printStackTrace();
        }
    }

}
