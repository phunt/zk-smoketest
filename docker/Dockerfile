FROM python:2.7-slim

RUN apt-get update
RUN apt-get install -y wget unzip
RUN apt-get install -y libzookeeper-mt-dev
RUN apt-get install -y gcc

RUN pip install zkpython

RUN wget https://github.com/phunt/zk-smoketest/archive/master.zip -O zk-smoketest.zip; \
    unzip zk-smoketest.zip; \
    rm zk-smoketest.zip

EXPOSE 2181

ENTRYPOINT ["zk-smoketest-master/zk-latencies.py"]
