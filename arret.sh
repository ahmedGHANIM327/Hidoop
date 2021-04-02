declare -a HDFSServer=( "magma" "bonite" "merry" "chewie" "zemanek" "atkinson" "torvalds")

NPROVIDER="galilee"

NBServer="7"

#stop running servers 
for (( i=0; i<$NBServer; i++ )); do
    ssh ${HDFSServer[$i]} "pkill -f java.*HdfsServerRun* "
done
#stop running workers 
for (( i=0; i<$NBServer; i++ )); do
    ssh ${HDFSServer[$i]} "pkill -f java.*WorkerImpl*"
done
# stop the nameProvider
ssh ${NPROVIDER} "pkill -f java.*NameProviderImpl*"
