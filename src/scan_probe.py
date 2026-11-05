"""Sondagem A/B da poda de colunas na leitura Parquet.

Le o ano completo duas vezes com uma agregacao sobre as colunas
numericas: uma sem poda (todas as colunas) e outra so com as 11
colunas usadas pelo pipeline. Reporta o tempo de parede e o tempo
de execucao dos executores por estagio, lidos da REST API da Spark UI.

Resultado observado nesta maquina (local[*], NVMe):
    ALL    : parede 2.3s, executores 9s
    PRUNED : parede 0.4s, executores 3s
"""

import json
import os
import time
import urllib.request

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

NEEDED = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
          "PULocationID", "DOLocationID", "passenger_count",
          "trip_distance", "RatecodeID", "payment_type",
          "fare_amount", "tip_amount", "total_amount"]


def stage_stats(spark, before_ids):
    sc = spark.sparkContext
    url = (f"http://localhost:{sc.uiWebUrl.rsplit(':', 1)[1]}"
           f"/api/v1/applications/{sc.applicationId}/stages")
    stages = [s for s in json.loads(urllib.request.urlopen(url).read())
              if s["stageId"] not in before_ids]
    return (sum(s.get("inputBytes", 0) for s in stages),
            sum(s.get("executorRunTime", 0) for s in stages) / 1000.0,
            sum(s.get("numTasks", 0) for s in stages))


def main():
    spark = (SparkSession.builder.master("local[*]")
             .appName("scan-probe")
             .config("spark.driver.memory", "6g")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    try:
        for tag, df in [("ALL", spark.read.parquet("data")),
                        ("PRUNED", spark.read.parquet("data").select(*NEEDED))]:
            sc = spark.sparkContext
            url = (f"http://localhost:{sc.uiWebUrl.rsplit(':', 1)[1]}"
                   f"/api/v1/applications/{sc.applicationId}/stages")
            before = {s["stageId"] for s in json.loads(urllib.request.urlopen(url).read())}
            num_cols = [c for c in df.columns
                        if df.schema[c].dataType.typeName() in ("double", "long", "integer")]
            t = time.time()
            df.agg(*[sf.sum(c).alias(c) for c in num_cols]).count()
            wall = time.time() - t
            inp, run, tasks = stage_stats(spark, before)
            print(f"{tag}: {len(num_cols)} colunas numericas, parede {wall:.1f}s, "
                  f"executores {run:.0f}s, {tasks} tasks")
    finally:
        spark.stop()


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    main()
