#!/bin/bash
source /opt/lsst/software/stack/loadLSST.bash
setup -r /home/lsst/ctrl_filterd
/home/lsst/ctrl_filterd/bin/filterd
