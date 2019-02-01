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
            python3-pandas \
            python3-pip \
            python3-psutil \
            python3-setuptools \
            python3-wheel \
            ipython3 \
            neovim \
            wget && \
    # build
    cd /opt && \
    git clone https://github.com/citp/BlockSci.git && \
    cd BlockSci && \
    git checkout "v0.6" && \
    git submodule init && \
    git submodule update --recursive && \
    mkdir release && \
    cd release && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make && \
    make install && \
    cd /opt/BlockSci && \
    # python
    pip3 install requests && \
    pip3 install cassandra-driver==3.16.0 && \
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
    mkdir -p /var/data/blocksci_data && \
    mkdir -p /var/data/block_data && \
    chown -R dockeruser /var/data/

COPY blocksci_export.py /usr/local/bin/blocksci_export.py

USER dockeruser
WORKDIR /home/dockeruser
