# NameProvider machine
NameProvider="galilee"

#  Directory of the sources
DIR="/mnt/nosave/clouahab/hidoop/src"

# Number of Servers
NBServer="7"

#Workers's Port
WORKERPORT="3000"

# Worker Address
declare -a Worker=( "magma" "bonite" "merry" "chewie" "zemanek" "atkinson" "torvalds")
declare -a HDFSServer=( "magma" "bonite" "merry" "chewie" "zemanek" "atkinson" "torvalds")

# Clean log file
rm nohup.out

# launch NameProvider
nohup ssh ${NameProvider} "cd ${DIR} && java hdfs.NameProviderImpl" & 
echo "NameProvider lancée"
sleep 2

# launch the servers
for (( i=0; i<$NBServer; i++ )); do
    nohup ssh ${HDFSServer[$i]}  "cd ${DIR} && java hdfs.HdfsServerRun ${HDFSServer[$i]}" &
done
echo "serveurs lancés"

# launch the workers
for (( i=0; i<$NBServer; i++ )); do
    nohup ssh ${Worker[$i]} "cd ${DIR} && java ordo.WorkerImpl ${Worker[$i]} ${WORKERPORT}" &
done
echo "workers lancés"

sleep 2
# Launch HDFS Client
nohup ssh ${NameProvider} "cd ${DIR} && java hdfs.HdfsClient write line ../data/data.txt && sleep 5 && java application.MyMapReduce data.txt"

