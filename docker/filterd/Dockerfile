ARG LSST_STACK_VERSION
FROM ghcr.io/lsst/scipipe:${LSST_STACK_VERSION}

WORKDIR /home/lsst

USER root
COPY . ctrl_filterd/
COPY docker/filterd/run.sh /home/lsst/run.sh
RUN chown lsst:lsst /home/lsst/run.sh
RUN chown -R lsst:lsst /home/lsst/ctrl_filterd
RUN mkdir -p /opt/rucio/etc


USER lsst

RUN git config --global --add safe.directory /home/lsst/ctrl_filterd && \
    source /opt/lsst/software/stack/loadLSST.bash && \
    pip install rucio-clients && \
    setup sconsUtils && \
    setup utils && \
    setup -r ctrl_filterd && \
    cd ctrl_filterd && \
    scons

ENTRYPOINT [ "/home/lsst/run.sh" ]
