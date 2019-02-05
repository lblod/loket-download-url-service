FROM semtech/mu-javascript-template:1.3.2
LABEL maintainer=info@redpencil.io

ENV FILE_PATH '/data/files/'
#ENV FILE_GRAPH 'http://mu.semte.ch/graphs/public'
RUN mkdir -p /data/files/