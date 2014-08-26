#!/bin/sh

set -e
set -x

TARBALL_DIR=$HOME/tarballs
OFFLINE_DISTSERVER_DIR=shared/dir/available/to/all/nodes

# OPTIONAL : cleaning of previous distribution server
mkdir -p ${TARBALL_DIR} 
mkdir -p ${OFFLINE_DISTSERVER_DIR} 
rm -f ${TARBALL_DIR}/qserv-offline-distserver.tar.gz
rm -rf ${OFFLINE_DISTSERVER_DIR} 

# prepare offline distribution server
cd ${TARBALL_DIR}
curl -O http://lsst-web.ncsa.illinois.edu/~fjammes/qserv-offline/qserv-offline-distserver.tar.gz
cd ${OFFLINE_DISTSERVER_DIR} 
tar zxvf ~/tarballs/qserv-offline-distserver.tar.gz

# OPTIONAL : python 2.7 is required, if it isn't available on you system, and
# if you can't install system packages, Anaconda may be a solution :
curl -O http://repo.continuum.io/archive/Anaconda-1.8.0-Linux-x86_64.sh
