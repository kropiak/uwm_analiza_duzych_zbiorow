#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM spark:4.1.4-scala2.13-java17-ubuntu

USER root

# Instalacja Python 3, pip oraz narzędzi wirtualnego środowiska
RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip python3-venv; \
    rm -rf /var/lib/apt/lists/*

# Przygotowanie katalogu domowego użytkownika spark oraz katalogu roboczego
RUN mkdir -p /home/spark /opt/spark/work-dir; \
    chown -R spark:spark /home/spark /opt/spark/work-dir

USER spark
WORKDIR /opt/spark/work-dir

# Preinstalacja pyspark 4.1.4, JupyterLab i requests w środowisku Pythona kontenera
ENV PATH="/home/spark/.local/bin:${PATH}"
RUN pip3 install --no-cache-dir --user pyspark==4.1.4 jupyterlab requests

EXPOSE 8888 4040 8080 7077

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''"]
