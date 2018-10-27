FROM ubuntu:18.04
LABEL maintainer="rainer.stuetz@ait.ac.at"

RUN useradd -m -d /home/dockeruser -r -u 10000 dockeruser && \
    apt-get update && \
    # install packages
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            autoconf \
            automake \
            build-essential \
            ca-certificates \
            cmake \
            git \
            #libboost-all-dev \
            libboost-atomic1.62.0 \
            libboost-atomic1.62-dev \
            libboost-chrono1.62.0 \
            libboost-chrono1.62-dev \
            libboost-date-time1.62.0 \
            libboost-date-time1.62-dev \
            libboost-filesystem1.62.0 \
            libboost-filesystem1.62-dev \
            libboost-iostreams1.62.0 \
            libboost-iostreams1.62-dev \
            libboost-program-options1.62.0 \
            libboost-program-options1.62-dev \
            libboost-regex1.62.0 \
            libboost-regex1.62-dev \
            libboost-system1.62.0 \
            libboost-system1.62-dev \
            libboost-serialization1.62.0 \
            libboost-serialization1.62-dev \
            libboost-thread1.62.0 \
            libboost-thread1.62-dev \
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
            python3-cassandra \
            python3-crypto \
            python3-pandas \
            python3-pip \
            python3-psutil \
            python3-requests \
            python3-setuptools \
            python3-wheel \
            ipython3 \
            neovim \
            wget && \
    # build
    cd /opt && \
    git clone https://github.com/citp/BlockSci.git && \
    cd BlockSci && \
    git submodule init && \
    git submodule update --recursive && \
    mkdir release && \
    cd release && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make && \
    make install && \
    cd /opt/BlockSci && \
    # python
    pip3 install -e blockscipy && \
    # clean up
    cd / && \
    mv /opt/BlockSci/blockscipy /opt/ && \
    rm -rf /opt/BlockSci/* && \
    mv /opt/blockscipy /opt/BlockSci && \
    rm -rf /opt/BlockSci/blockscipy/build /root/.cache && \
    apt-get autoremove -y --purge \
            autoconf \
            automake \
            build-essential \
            git \
            libboost-atomic1.62-dev \
            libboost-chrono1.62-dev \
            libboost-date-time1.62-dev \
            libboost-filesystem1.62-dev \
            libboost-iostreams1.62-dev \
            libboost-program-options1.62-dev \
            libboost-regex1.62-dev \
            libboost-system1.62-dev \
            libboost-serialization1.62-dev \
            libboost-thread1.62-dev \
            libjsoncpp-dev \
            libjsonrpccpp-dev \
            libjsonrpccpp-tools \
            libssl-dev \
            libsparsehash-dev \
            libtool && \
    mkdir -p /var/data/blocksci_data/bitcoin && \
    mkdir -p /var/data/blocksci_data/bitcoincash && \
    mkdir -p /var/data/block_data/bitcoin && \
    mkdir -p /var/data/block_data/bitcoincash && \
    mkdir -p /var/data/block_data/litecoin && \
    mkdir -p /var/data/block_data/zcash && \
    chown -R dockeruser /var/data/

USER dockeruser
WORKDIR /home/dockeruser
