FROM ubuntu:20.04 as builder
LABEL maintainer="contact@graphsense.info"

# install dependencies
RUN ln -snf /usr/share/zoneinfo/UTC /etc/localtime && \
  echo UTC > /etc/timezone && \
  apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  autoconf \
  automake \
  build-essential \
  clang-7 \
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
  python3.8 \
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
  git submodule init && \
  git submodule update --recursive

# apply patches
COPY patches/0001-Changing-tx-version-to-Uint-LTC-uses-2-32-1-as-tx-ve.patch /opt/BlockSci/external/bitcoin-api-cpp

RUN cd /opt/BlockSci/external/bitcoin-api-cpp && git apply 0001-Changing-tx-version-to-Uint-LTC-uses-2-32-1-as-tx-ve.patch

# build
RUN cd /opt/BlockSci && \
  export CC=/usr/bin/clang-7 && \
  export CXX=/usr/bin/clang++-7 && \
  mkdir release && \
  cd release && \
  cmake -DCMAKE_BUILD_TYPE=Release .. && \
  make && \
  make install

COPY requirements-docker.txt /tmp/requirements.txt

# install Python packages
RUN cd /opt/BlockSci && \
  export CC=/usr/bin/clang-7 && \
  export CXX=/usr/bin/clang++-7 && \
  pip3 install -r /tmp/requirements.txt && \
  pip3 install -e blockscipy

# cleanup
RUN cd / && \
  mv /opt/BlockSci/blockscipy /opt/ && \
  rm -rf /opt/BlockSci/* && \
  mv /opt/blockscipy /opt/BlockSci

FROM ubuntu:20.04

COPY --from=builder /opt/BlockSci/blockscipy/blocksci /usr/local/lib/python3.8/dist-packages/blocksci
COPY --from=builder /usr/bin/blocksci_* /usr/local/bin/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libblocksci.so /usr/local/lib/
COPY --from=builder /usr/local/lib/python3.8/dist-packages /usr/local/lib/python3.8/dist-packages
COPY ./docker/docker-entrypoint.sh /
COPY --from=builder /opt/BlockSci/ /opt/BlockSci/

RUN useradd -m -d /home/dockeruser -r -u 10000 dockeruser && \
  apt-get update && \
  # install packages
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  ipython3 \
  libboost-serialization1.71.0 \
  libjsoncpp1 \
  libjsonrpccpp-client0 \
  libssl1.1 \
  neovim \
  python3-bs4 \
  python3-crypto \
  python3-lxml \
  python3-pandas \
  python3-pip \
  gdb \
  python3-psutil && \
  mkdir -p /var/data/blocksci_data && \
  mkdir -p /var/data/block_data && \
  chown -R dockeruser /var/data/ && \
  chmod +x /docker-entrypoint.sh

COPY scripts/*.py /usr/local/bin/
COPY scripts/*.sh /usr/local/bin/
COPY scripts/schema.cql /opt/graphsense/schema.cql

USER dockeruser
WORKDIR /home/dockeruser

CMD ["/bin/bash"]
