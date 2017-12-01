#!/bin/sh

# Launch Qserv pods on Kubernetes cluster

# @author  Fabrice Jammes, IN2P3/SLAC

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$HOME/.kube/env.sh"

CFG_DIR="${DIR}/yaml"
RESOURCE_DIR="${DIR}/resource"

# For in2p3 cluster: k8s schema cache must not be on AFS
TMP_DIR=$(mktemp -d --suffix=-kube-$USER)
SCHEMA_CACHE_OPT="--schema-cache-dir=$TMP_DIR/schema"

usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -h          this message

  Launch Qserv service and pods on Kubernetes

EOD
}

# get the options
while getopts h c ; do
    case $c in
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

YAML_MASTER_TPL="${CFG_DIR}/pod.master.yaml.tpl"
YAML_FILE="${CFG_DIR}/master.yaml"
INI_FILE="${CFG_DIR}/pod.master.ini"

cat << EOF > "$INI_FILE"
[spec]
host_custom_dir: $HOST_CUSTOM_DIR
host_data_dir: $HOST_DATA_DIR
host_log_dir: $HOST_LOG_DIR
host_tmp_dir: $HOST_TMP_DIR
host: $MASTER
image: $CONTAINER_IMAGE
image_mariadb: qserv/mariadb_scisql:$MARIADB_VERSION
master_hostname: $MASTER
pod_name: master
EOF

"$DIR"/yaml-builder.py -i "$INI_FILE" -r "$RESOURCE_DIR" -t "$YAML_MASTER_TPL" -o "$YAML_FILE"

echo "Create kubernetes configmap for sql files"
kubectl delete configmap --ignore-not-found=true config-sql
kubectl create configmap --from-file="$RESOURCE_DIR/cm_sql" config-sql

echo "Create kubernetes configmap for Qserv master"
tar -zcvf "$RESOURCE_DIR/cm_master.tgz" -C "$RESOURCE_DIR/cm_master" .
kubectl delete configmap --ignore-not-found=true config-master
kubectl create configmap --from-file="$RESOURCE_DIR/cm_master" config-master

echo "Create kubernetes pod for Qserv master"
kubectl create $SCHEMA_CACHE_OPT -f "$YAML_FILE"

YAML_WORKER_TPL="${CFG_DIR}/pod.worker.yaml.tpl"
j=1
for host in $WORKERS;
do
    YAML_FILE="${CFG_DIR}/worker-${j}.yaml"
    INI_FILE="${CFG_DIR}/pod.worker-${j}.ini"
    cat << EOF > "$INI_FILE"
[spec]
host_custom_dir: $HOST_CUSTOM_DIR
host_data_dir: $HOST_DATA_DIR
host_log_dir: $HOST_LOG_DIR
host_tmp_dir: $HOST_TMP_DIR
host: $host
image: $CONTAINER_IMAGE
image_mariadb: qserv/mariadb_scisql:$MARIADB_VERSION
master_hostname: $MASTER
mysql_root_password: CHANGEME
pod_name: worker-$j
EOF
    "$DIR"/yaml-builder.py -i "$INI_FILE" -r "$RESOURCE_DIR" -t "$YAML_WORKER_TPL" -o "$YAML_FILE"
    echo "Create kubernetes pod for Qserv worker-${j}"
    kubectl create $SCHEMA_CACHE_OPT -f "$YAML_FILE"
    j=$((j+1));
done
