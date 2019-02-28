#!/bin/bash

set -e

usage="usage: <ids_per_chunk> <stride>"
if [ $# -ne 2 ]
then
  echo $usage
  exit 1
else
  if [ -z "$1" ]
  then
    echo $usage
    exit 1
  else
    ids_per_chunk="$1"
  fi
  if [ -z "$2" ]
  then
    echo $usage
    exit 1
  else
    stride="$2"
  fi
fi

source /sps/lsst/Qserv/stack/loadLSST.bash
setup mariadbclient

mysql_cmd="mysql -S /qserv/data/mysql/mysql.sock -u qsmaster"

${mysql_cmd} -e "SELECT DISTINCT chunkId FROM qservMeta.LSST30__Object" > chunks_LSST30.txt

for iter in `seq 0 $((ids_per_chunk-1))`
do
  offset=$((stride*iter))
  for chunkId in `cat chunks_LSST30.txt`
  do
    deepSourceId=`${mysql_cmd} -e "SELECT deepSourceId FROM qservMeta.LSST30__Object WHERE chunkId='$chunkId' LIMIT 1 OFFSET $offset"`
    echo $deepSourceId
  done
done
