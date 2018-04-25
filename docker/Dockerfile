FROM debian:buster-slim
LABEL maintainer="rainer.stuetz@ait.ac.at"

RUN useradd -m -d /home/dockeruser -r -u 1008 dockeruser && \
    echo "deb http://ftp.debian.org/debian stretch-backports main" >> /etc/apt/sources.list && \
    apt-get update && \
    # install packages
    apt-get install -y --no-install-recommends \
            build-essential \
            ca-certificates \
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
            libdpkg-perl \
            libgflags-dev \
            libjsoncpp-dev \
            libjsonrpccpp-client0 \
            libjsonrpccpp-common0 \
            libjsonrpccpp-dev \
            libjsonrpccpp-tools \
            libpython3-dev \
            librocksdb-dev \
            librocksdb5.11 \
            libsecp256k1-0 \
            libsecp256k1-dev \
            libsparsehash-dev \
            libssl-dev \
            python3.6 \
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
    apt-get -y -t stretch-backports install cmake && \
    # build
    cd /opt && \
    git clone https://github.com/mpark/variant.git && \
    mkdir variant/build && cd variant/build && \
    cmake .. && \
    cmake --build . --target install && \
    cd /opt && \
    git clone https://github.com/citp/BlockSci.git && \
    cd BlockSci && \
    git submodule init && \
    git submodule update --recursive && \
    cp -r libs/range-v3/include/meta /usr/local/include && \
    cp -r libs/range-v3/include/range /usr/local/include && \
    cd libs/bitcoin-api-cpp && \
    mkdir release && \
    cd release && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make && \
    make install && \
    cd ../../.. && \
    mkdir release && \
    cd release && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make && \
    make install && \
    cd ../clustering && \
    mkdir release && \
    cd release && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make && \
    make install && \
    cd ../.. && \
    # python
    pip3 install dateparser multiprocess && \
    # clean up
    cd / && \
    mkdir /opt/python && \
    mv /opt/BlockSci/Notebooks/blocksci /opt/python && \
    rm -rf /opt/BlockSci /opt/variant && \
    apt-get autoremove -y --purge \
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
            libdpkg-perl \
            libgflags-dev \
            libjsoncpp-dev \
            libjsonrpccpp-dev \
            libjsonrpccpp-tools \
            libssl-dev \
            librocksdb-dev \
            libsecp256k1-dev \
            libsparsehash-dev && \
    ln -s /opt/python/blocksci /usr/local/lib/python3.6/dist-packages/blocksci && \
    mkdir -p /var/data/blocksci_data && \
    mkdir -p /var/data/block_data

USER dockeruser
WORKDIR /home/dockeruser
