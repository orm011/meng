PIDS=""
PIDFILE=$PWD/node.pid
LOGSDIR=$PWD/logs/

if [[ ! -d $LOGSDIR && ! -L $LOGSDIR ]]; then 
    mkdir $LOGSDIR
fi

#need to start rmiregistry in ./bin for some reason
cd ./bin/
rmiregistry & >> $LOGSDIR/rmi.log
echo $! >> $PIDFILE
PIDS="$! $PIDS"
cd ../

NODES="node0 node1 node2 node3 node4"
SECURITYPOLICY="-Djava.security.policy=server.policy"
CP="-cp ./bin/"
CLASS="com.twitter.dataservice.simulated.WorkNodeMain"

#start each node
for NODE in $NODES
do
    if [ -e $LOGSDIR/$NODE.log ]; then
	echo "warning: logfile $NODE.log already exists in $LOGSDIR"
    fi

    java $CP $SECURITYPOLICY $CLASS $NODE 2>&1 >> $LOGSDIR/$NODE.log &
    echo $! >> $PIDFILE
    PIDS="$! $PIDS"
done

#wait for each of the nodes to bind before starting apiserver
for NODE in $NODES
do
    while ! grep -q "success" $LOGSDIR/$NODE.log
    do 
	echo "waiting for $NODE to bind..."
	sleep 1
    done 
    echo "done with $NODE"
done
echo "done binding."

CODEBASEFLAG="-Djava.rmi.server.codebase=file://remotes.jar"
APISERVERCLASS="com.twitter.dataservice.simulated.APIServer"
java $CP $CODEBASEFLAG $SECURITYPOLICY $APISERVERCLASS $NODES 2>&1 >> $LOGSDIR/apiserver.log &
echo $! >> $PIDFILE
wait $!

echo '--------------'
echo 'API sever log:'
cat $LOGSDIR/apiserver.log