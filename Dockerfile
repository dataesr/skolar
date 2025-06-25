FROM ubuntu:18.04
ARG DEBIAN_FRONTEND=noninteractive

RUN  apt-get update \
  && apt-get install -y wget \
     gnupg2

RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | apt-key add -

RUN echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/5.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-5.0.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    g++ \
    git \
    groff \
    jq \
    less \
    libpython3.8 \
    libpython3.8-dev \
    locales \
    locales-all \
    mongodb-org \
    npm \
    make \
    python3-dev \
    python3-pip \
    python3-setuptools \
    python3.8 \
    python3.8-dev \
    unzip \
    zip \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

#RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3.8 get-pip.py
RUN curl https://bootstrap.pypa.io/pip/3.8/get-pip.py -o get-pip.py && python3.8 get-pip.py

# AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws/

# OVHAI CLI
RUN curl https://cli.gra.ai.cloud.ovh.net/install.sh | bash

WORKDIR /src

ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8

COPY requirements.txt /src/requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --proxy=${HTTP_PROXY}

COPY . /src
