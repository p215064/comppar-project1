#!/bin/bash
# Arranque do pipeline com as variaveis necessarias no Windows.
# O PySpark constroi o java.library.path a partir do PATH, pelo que a
# pasta com winutils.exe / hadoop.dll tem de estar no PATH (e nao so
# no HADOOP_HOME).
set -e
cd "$(dirname "$0")"
export JAVA_HOME='C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot'
export HADOOP_HOME='C:\Users\rodri\Desktop\comppar\CompPar\project1\tools\winutils'
export PATH="$HADOOP_HOME/bin:$PATH"
python src/pipeline.py "$@"
