#!/bin/bash
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
setup -r /home/lsst/ctrl_filterd
/home/lsst/ctrl_ingestd/bin/filterd
