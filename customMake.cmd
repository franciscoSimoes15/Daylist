@echo off
set BASE_IMAGE=bigdata25/jupyter:v5
set IMAGE=custom-jupyter-v5

rem --- BAKE ROCK ---
mkdir jupyter 2>nul
echo FROM %BASE_IMAGE% > jupyter\Dockerfile

rem 1. Root for apt
echo USER root >> jupyter\Dockerfile
echo RUN apt-get update ^&^& apt-get install -y socat >> jupyter\Dockerfile

rem 2. User for pip/files (try jovyan if hadoop fails)
echo USER hadoop >> jupyter\Dockerfile
echo RUN pip install --upgrade pip setuptools wheel >> jupyter\Dockerfile
echo RUN pip install implicit fastapi uvicorn redis mlflow scikit-learn websockets --no-cache-dir pyhive thrift thrift-sasl >> jupyter\Dockerfile
echo RUN echo '^<configuration^>^</configuration^>' ^> /bigdata/hadoop-3.3.6/etc/hadoop/mapred-site.xml >> jupyter\Dockerfile

rem --- BUILD ---
echo docker build -t %IMAGE% jupyter
docker build -t %IMAGE% jupyter

rem --- RUN ---
FOR /F "tokens=4 delims= " %%i in ('route print ^| find " 0.0.0.0"') do set HOSTIP=%%i
echo Your IP Address is: %HOSTIP%

docker run -d -p 8888:8888 -p 8000:8000 --hostname %HOSTIP% -e HADOOP_MASTER_IP=%HADOOP_MASTER_IP% --mount src="%CD%\data",dst=/home/hadoop/data/,type=bind %IMAGE%