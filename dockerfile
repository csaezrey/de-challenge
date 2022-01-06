FROM oracleinanutshell/oracle-xe-11g
#PATHS
RUN mkdir /home/data
RUN mkdir /home/src
RUN mkdir -p /opt/oracle
RUN mkdir -p /home/src/log/

#MODULES
RUN apt update -y
RUN apt install python3 -y
RUN apt install python3-pip -y
RUN apt-get -y install cron
RUN apt-get install unzip -y
RUN apt-get install libaio1 libaio-dev

#MODULES PYTHON
RUN pip3 install pandas
RUN pip3 install cx_Oracle
RUN pip3 install sqlalchemy

#ENVIRONMENT 
ENV export PYTHONPATH="etc/python3"
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_4:$LD_LIBRARY_PATH

#SCRIPT DATABASE
COPY /script/init.sql /docker-entrypoint-initdb.d/
COPY /data /home/data

#LOAD CODE
COPY /src /home/src

#CRON
ADD crontab /etc/cron.d/etl
RUN chmod 0644 /etc/cron.d/etl
RUN crontab /etc/cron.d/etl
RUN touch /home/src/log/cron.log

#ORACLE CLIENT
WORKDIR /opt/oracle
RUN sh -c "echo /opt/oracle/instantclient_21_4 > /etc/ld.so.conf.d/oracle-instantclient.conf"
ADD instantclient-basic-linux.x64-21.4.0.0.0dbru.zip /opt/oracle
RUN unzip instantclient-basic-linux.x64-21.4.0.0.0dbru.zip
RUN ldconfig

#PORT DATABASE
EXPOSE 1521