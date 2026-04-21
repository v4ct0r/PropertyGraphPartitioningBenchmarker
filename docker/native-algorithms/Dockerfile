FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    bison \
    build-essential \
    ca-certificates \
    cmake \
    file \
    flex \
    git \
    libopenmpi-dev \
    openmpi-bin \
    pkg-config \
    python3 \
    python3-openpyxl \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/src

COPY GKlib /opt/src/GKlib
COPY METIS /opt/src/METIS
COPY ParMETIS /opt/src/ParMETIS
COPY KaHIP /opt/src/KaHIP
COPY scotch-7.0.4 /opt/src/scotch-7.0.4

RUN cd /opt/src/GKlib \
    && rm -rf build \
    && make config prefix=/opt/algorithms/gklib \
    && make -j"$(nproc)" \
    && make install

RUN cd /opt/src/METIS \
    && rm -rf build \
    && make config shared=1 gklib_path=/opt/algorithms/gklib prefix=/opt/algorithms/metis \
    && make -j"$(nproc)" \
    && make install

RUN cd /opt/src/ParMETIS \
    && rm -rf build \
    && make config cc=mpicc gklib_path=/opt/algorithms/gklib metis_path=/opt/algorithms/metis prefix=/opt/algorithms/parmetis \
    && make -j"$(nproc)" \
    && make install \
    && if [ -x /opt/algorithms/parmetis/bin/pm_parmetis ]; then \
         ln -sf /opt/algorithms/parmetis/bin/pm_parmetis /opt/algorithms/parmetis/bin/parmetis; \
       fi

RUN cd /opt/src/KaHIP \
    && rm -rf build deploy \
    && cmake -B build -DCMAKE_BUILD_TYPE=Release . \
    && cmake --build build -j"$(nproc)" \
    && mkdir -p /opt/algorithms/kahip/bin \
    && cp build/kaffpa /opt/algorithms/kahip/bin/kaffpa

RUN cd /opt/src/scotch-7.0.4/src \
    && cp Make.inc/Makefile.inc.x86-64_pc_linux2 Makefile.inc \
    && sed -i 's/^CCD.*/CCD\t\t= mpicc/' Makefile.inc || true \
    && sed -i 's/^CCP.*/CCP\t\t= mpicc/' Makefile.inc || true \
    && sed -i 's/^CFLAGS\t\+=/CFLAGS\t\t=/' Makefile.inc || true \
    && make scotch ptscotch -j"$(nproc)" \
    && mkdir -p /opt/algorithms/scotch/bin \
    && for bin in gcv gpart dgpart scotchmetis ptscotchmetis; do \
         if [ -x "/opt/src/scotch-7.0.4/src/scotch/${bin}" ]; then \
           cp "/opt/src/scotch-7.0.4/src/scotch/${bin}" /opt/algorithms/scotch/bin/; \
         fi; \
       done

ENV PATH="/opt/algorithms/metis/bin:/opt/algorithms/parmetis/bin:/opt/algorithms/kahip/bin:/opt/algorithms/scotch/bin:${PATH}"
ENV LD_LIBRARY_PATH="/opt/algorithms/gklib/lib:/opt/algorithms/metis/lib:/opt/algorithms/parmetis/lib"

CMD ["/bin/bash"]
