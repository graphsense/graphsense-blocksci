FROM ubuntu:18.04 as builder
LABEL maintainer="contact@graphsense.info"

# install dependencies
RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  autoconf \
  automake \
  build-essential \
  ca-certificates \
  cmake \
  git \
  libboost-all-dev \
  liblz4-dev \
  libtool \
  libjsoncpp-dev \
  libjsonrpccpp-client0 \
  libjsonrpccpp-common0 \
  libjsonrpccpp-dev \
  libjsonrpccpp-tools \
  libpython3-dev \
  libsparsehash-dev \
  libssl-dev \
  python3.6 \
  python3-crypto \
  python3-pip \
  python3-psutil \
  python3-setuptools \
  python3-wheel \
  wget

# add BlockSci
RUN cd /opt && \
  git clone https://github.com/citp/BlockSci.git && \
  cd BlockSci && \
  git checkout "v0.6" && \
  git submodule init && \
  git submodule update --recursive

# build
RUN cd /opt/BlockSci && \
  mkdir release && \
  cd release && \
  cmake -DCMAKE_BUILD_TYPE=Release .. && \
  make && \
  make install

COPY requirements-docker.txt /tmp/requirements.txt

# install Python packages
RUN cd /opt/BlockSci && \
  pip3 install -r /tmp/requirements.txt && \
  pip3 install -e blockscipy

# cleanup
RUN cd / && \
  mv /opt/BlockSci/blockscipy /opt/ && \
  rm -rf /opt/BlockSci/* && \
  mv /opt/blockscipy /opt/BlockSci

FROM ubuntu:18.04

COPY --from=builder /opt/BlockSci/blockscipy/blocksci /usr/local/lib/python3.6/dist-packages/blocksci
COPY --from=builder /usr/bin/blocksci_* /usr/local/bin/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libblocksci.so /usr/local/lib/
COPY --from=builder /usr/local/lib/python3.6/dist-packages /usr/local/lib/python3.6/dist-packages
COPY scripts/blocksci_export.py /usr/local/bin/blocksci_export.py

RUN useradd -m -d /home/dockeruser -r -u 10000 dockeruser && \
  apt-get update && \
  # install packages
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  ipython3 \
  libjsoncpp1 \
  libjsonrpccpp-client0 \
  libssl1.1 \
  neovim \
  python3-crypto \
  python3-pandas \
  python3-pip \
  python3-psutil && \
  mkdir -p /var/data/blocksci_data && \
  mkdir -p /var/data/block_data && \
  chown -R dockeruser /var/data/

USER dockeruser
WORKDIR /home/dockeruser
