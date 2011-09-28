for i in $(cat node.pid); do echo $i; kill -9 $i; done
rm ./logs/*
rm *.pid
