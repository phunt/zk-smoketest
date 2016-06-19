FROM python:2.7.11

RUN apt-get update && \
    apt-get install -y wget unzip && \
    apt-get install -y libzookeeper-mt-dev && \
    pip install zkpython

RUN wget https://github.com/phunt/zk-smoketest/archive/master.zip -O zk-smoketest.zip; \
    unzip zk-smoketest.zip; \
    rm zk-smoketest.zip

EXPOSE 2181

ENTRYPOINT ["zk-smoketest-master/zk-latencies.py"]