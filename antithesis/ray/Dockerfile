FROM ubuntu:22.04

RUN useradd -ms /bin/bash ray

# install dependencies
RUN apt-get update
RUN apt-get install -y git build-essential libc6-dev libjemalloc-dev zlib1g-dev clang lld pkg-config psmisc curl unzip xsltproc lua5.4
RUN apt-get install -y python-is-python3 python3-pip
RUN apt-get install -y nodejs npm

USER ray
RUN git clone "https://github.com/antithesishq/ray.git" /ray

# install bazelisk
WORKDIR /home/ray/ray
RUN ci/env/install-bazel.sh
ENV PATH="$PATH:/home/ray/bin:/home/ray/.local/bin"

# build dashboard
WORKDIR /home/ray/ray/python/ray/dashboard/client
RUN npm ci
RUN npm run build

# build python package
WORKDIR /home/ray/ray/python
ENV RAY_DEBUG_BUILD="debug" CC="clang"
RUN pip install -r requirements.txt
RUN pip install . --verbose
