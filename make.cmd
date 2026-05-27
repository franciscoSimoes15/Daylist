@echo off
set IMAGE=bigdata25/jupyter:v5

echo docker build  -t %IMAGE% jupyter
docker build  -t %IMAGE% jupyter
rem run:
rem export MASTERIP=10.84.129.52
rem export HADOOP_MASTER_IP=$MASTERIP
rem echo HDFS IP Address is: $HADOOP_MASTER_IP
rem export HOSTIP=10.204.131.100
rem echo Your IP Address is: ${HOSTIP}
rem docker run --rm -d --network host  --mount src=${PWD}/data,dst=/data/spark/,type=bind ${IMAGE}
rem docker run --rm -d --net host  --mount src=./data,dst=/data/spark/,type=bind ${IMAGE}
rem docker run --rm -d --network host  --mount src=%CD%\data,dst=/data/spark/,type=bind %IMAGE%

rem echo docker run --rm -d --net host  --mount src=%CD%\data,dst=/data/spark/,type=bind %IMAGE%
rem docker run --rm -d --net host  --mount src=%CD%\data,dst=/data/spark/,type=bind %IMAGE%

rem @for /f "delims=[] tokens=2" %%a in ('ping -4 -n 1 %ComputerName% ^| findstr [') do (
rem     set "MY_IP=%%a"
rem )
rem echo HOSTIP=%MY_IP%
rem echo docker run --rm -d -hostaname %MYIP% --net host  --mount src=%CD%\data,dst=/data/spark/,type=bind %IMAGE%
rem docker run --rm -d -hostaname %MYIP% -p 8888:8888  --mount src=%CD%\data,dst=/data/spark/,type=bind %IMAGE%

rem HOSTIP=10.204.128.91
FOR /F "tokens=4 delims= " %%i in ('route print ^| find " 0.0.0.0"') do set HOSTIP=%%i
echo Your IP Address is: %HOSTIP%

rem docker run --rm -it  -p 8888:8888 --hostname %HOSTIP% -e HADOOP_MASTER_IP=%HADOOP_MASTER_IP%  jupyt  
docker run -d -p 8888:8888 -p 8000:8000 --hostname %HOSTIP% -e HADOOP_MASTER_IP=%HADOOP_MASTER_IP%  --mount src="%CD%\data",dst=/home/hadoop/data/,type=bind %IMAGE%
rem docker run --rm -d -P --net=host --hostname %HOSTIP% -e HADOOP_MASTER_IP=%HADOOP_MASTER_IP%  --mount src="%CD%\data",dst=/home/hadoop/data/,type=bind %IMAGE%
  
