@echo off
set BASE_IMAGE=bigdata25/jupyter:v5
set IMAGE=custom-jupyter-v5
set APP_CONTAINER=big-data-music
set REDIS_CONTAINER=redis-server
set DOCKER_NETWORK=big-data-music-net

rem --- BAKE IMAGE ---
mkdir jupyter 2>nul
echo FROM %BASE_IMAGE% > jupyter\Dockerfile

rem 1. Root for apt
echo USER root >> jupyter\Dockerfile
echo RUN apt-get update ^&^& apt-get install -y socat openjdk-11-jdk ^&^& rm -rf /var/lib/apt/lists/* >> jupyter\Dockerfile
echo ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 >> jupyter\Dockerfile
echo ENV PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH >> jupyter\Dockerfile
echo RUN mkdir -p /usr/java ^&^& ln -sfn /usr/lib/jvm/java-11-openjdk-amd64 /usr/java/default >> jupyter\Dockerfile

rem Prevent bash startup error if base image references this file.
echo RUN mkdir -p /home/hadoop/bin ^&^& touch /home/hadoop/bin/setvars.sh ^&^& chmod +x /home/hadoop/bin/setvars.sh ^&^& chown -R hadoop:hadoop /home/hadoop/bin >> jupyter\Dockerfile

rem 2. User for pip/files
echo USER hadoop >> jupyter\Dockerfile
echo RUN pip install --upgrade pip setuptools wheel >> jupyter\Dockerfile
echo RUN pip install implicit fastapi uvicorn redis mlflow scikit-learn websockets kafka-python --no-cache-dir pyhive thrift thrift-sasl >> jupyter\Dockerfile
rem Keep Hadoop config from base image. Do not wipe mapred-site.xml.

rem --- BUILD ---
echo docker build -t %IMAGE% jupyterv
docker build -t %IMAGE% jupyter

rem --- NETWORK ---
docker network inspect %DOCKER_NETWORK% >nul 2>nul
if errorlevel 1 docker network create %DOCKER_NETWORK%

rem --- REMOVE OLD CONTAINERS WITH SAME NAMES ---
docker rm -f %APP_CONTAINER% >nul 2>nul
docker rm -f %REDIS_CONTAINER% >nul 2>nul

rem --- RUN REDIS ---
docker run -d ^
  --name %REDIS_CONTAINER% ^
  --network %DOCKER_NETWORK% ^
  -p 6379:6379 ^
  -v redis_data:/data ^
  redis:7-alpine ^
  redis-server --save 60 1 --appendonly yes

rem --- RUN APP CONTAINER ---
FOR /F "tokens=4 delims= " %%i in ('route print ^| find " 0.0.0.0"') do set HOSTIP=%%i
echo Your IP Address is: %HOSTIP%
if "%HADOOP_MASTER_IP%"=="" set HADOOP_MASTER_IP=10.84.129.52

docker run -d ^
  --name %APP_CONTAINER% ^
  --network %DOCKER_NETWORK% ^
  --add-host hivemetastore:10.84.128.48 ^
  -p 8888:8888 ^
  -p 8000:8000 ^
  -e HADOOP_MASTER_IP=%HADOOP_MASTER_IP% ^
  -e HIVE_SERVER2_IP=10.84.128.48 ^
  -e HIVE_SERVER2_PORT=10000 ^
  -e HIVE_DATABASE=francisco_jose_simoes ^
  -e HIVE_METASTORE_URIS=thrift://10.84.128.48:9083 ^
  -e HDFS_NAMENODE_HTTP=http://10.84.129.52:9870 ^
  -e KAFKA_SERVERS=10.204.131.11:9092,10.84.128.10:9092 ^
  -e KAFKA_BOOTSTRAP_SERVERS=10.204.131.11:9092,10.84.128.10:9092 ^
  -e KAFKA_TOPICS=music.events.play,music.events.skip,music.events.like ^
  -e KAFKA_STRICT_HEALTH=false ^
  -e KAFKA_CREATE_TOPICS=false ^
  -e REDIS_HOST=%REDIS_CONTAINER% ^
  -e REDIS_PORT=6379 ^
  -e ALLOW_HIVE_RECOMMEND_FALLBACK=false ^
  -e REDIS_TTL_SECONDS=3600 ^
  -e REDIS_RECS_TTL_SECONDS=86400 ^
  -e PRECOMPUTE_TOP_N=50 ^
  -e WRITE_REDIS_PRECOMPUTED=true ^
  -e MODEL_PATH=/home/hadoop/data/mbdump_small/models/als_model.pkl ^
  -e FEAT_PATH=/home/hadoop/data/mbdump_small/features/implicit.tsv ^
  -e FEATURE_DIR=/home/hadoop/data/mbdump_small/features ^
  -e MODEL_DIR=/home/hadoop/data/mbdump_small/models ^
  -e SPARK_CHECKPOINT_LOCATION=/home/hadoop/data/mbdump_small/checkpoints/music_events_all_v1 ^
  -e SPARK_CHECKPOINT_PATH=/home/hadoop/data/mbdump_small/checkpoints/music_events_all_v1 ^
  --mount src="%CD%",dst=/home/hadoop/data/,type=bind ^
  %IMAGE%

echo.
echo Containers started:
echo   App:   %APP_CONTAINER%
echo   Redis: %REDIS_CONTAINER%
echo.
echo Test Redis from app container:
echo   docker exec -it %APP_CONTAINER% python -c "import redis; r=redis.Redis(host='redis-server',port=6379); print(r.ping())"
echo.
echo Enter app container:
echo   docker exec -it %APP_CONTAINER% bash
