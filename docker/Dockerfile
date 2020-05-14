FROM hysds/pge-base:develop-es1

MAINTAINER malarout "Justin.P.Linick@jpl.nasa.gov"
LABEL description="PGE for AOI Trigger Rules Creation & Job submission"

USER ops

#install shapely
RUN sudo pip install shapely

# provision create_AOI PGE
COPY . /home/ops/verdi/ops/create_aoi_rules

# set entrypoint
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
