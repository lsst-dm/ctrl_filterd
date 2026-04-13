ARG LSST_STACK_VERSION
FROM ghcr.io/lsst/scipipe:${LSST_STACK_VERSION}

WORKDIR /home/lsst
RUN source /opt/lsst/software/stack/loadLSST.bash && \
    mamba install rucio-clients

COPY run.sh /home/lsst/run.sh

ENTRYPOINT [ "/usr/bin/tail", "-f", "/dev/null" ]

