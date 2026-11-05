"""
pipeline.py
-----------
Pipeline PySpark para o Project 1 de ComPar: analise de desempenho sobre os
dados de taxis amarelos de Nova Iorgue (NYC TLC, 2024).

Operacoes implementadas:
    M1  - ingestao e limpeza dos dados (estatisticas por ano)
    M2  - analise temporal da procura por (borough, dia da semana, hora)
    M3  - estatisticas por zona com funcoes de janela
    O1  - deteccao de anomalias no preco por milha
    O2  - analise de fluxos origem-destino

Cada otimizacao pode ser ligada/desligada para medir o seu efeito isolado.
As metricas de cada operacao (tempo de parede, bytes de shuffle, numero de
tasks, etc.) sao gravadas em results/<variante>.json a medida que vao sendo
medidas, para que uma corrida interrompida possa ser retomada com --resume.

Exemplos de uso (a correr a partir da raiz do project1):

    python src/pipeline.py --variant baseline --ops m1,m2,m3,o1,o2
    python src/pipeline.py --variant final --ops m1,m2,m3,o1,o2 --write-all
    python src/pipeline.py --variant final --ops o1 --sample 0.2
"""

import argparse
import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as sf
from pyspark.sql.types import DoubleType
from spark_profiler import SparkProfiler

# ---------------------------------------------------------------------------
# Variantes: cada chave diz quais otimizacoes estao LIGADAS nessa corrida.
# "baseline" e a versao ingenua (tudo desligado).
# ---------------------------------------------------------------------------
VARIANTS = {
    "baseline": dict(prune=False, broadcast=False, cache=False,
                     vectorized=False, bstats=False),
    "final":    dict(prune=True, broadcast=True, cache=True,
                     vectorized=True, bstats=True),
    # variantes de controlo: final com uma unica otimizacao desligada
    "noprun":   dict(prune=False, broadcast=True, cache=True,
                     vectorized=True, bstats=True),
    "nobroadcast": dict(prune=True, broadcast=False, cache=True,
                        vectorized=True, bstats=True),
    "nocache":  dict(prune=True, broadcast=True, cache=False,
                     vectorized=True, bstats=True),
    "novector": dict(prune=True, broadcast=True, cache=True,
                     vectorized=False, bstats=True),
    "nobstats": dict(prune=True, broadcast=True, cache=True,
                     vectorized=True, bstats=False),
}

NEEDED_COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "passenger_count",
    "trip_distance", "RatecodeID", "payment_type",
    "fare_amount", "tip_amount", "total_amount",
]

PAYMENT_LABELS = {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute"}

# limites do O4/O1 ficam documentados; o O2 usa trimestres fixos


def parse_args():
    p = argparse.ArgumentParser(description="Pipeline ComPar Project 1")
    p.add_argument("--variant", default="final", choices=list(VARIANTS) + ["any"])
    p.add_argument("--ops", default="m1,m2,m3,o1,o2")
    p.add_argument("--input", default="data")
    p.add_argument("--zones", default="data/zones/taxi_zone_lookup.csv")
    p.add_argument("--out", default="output")
    p.add_argument("--results", default="results")
    p.add_argument("--sample", type=float, default=1.0,
                   help="fracao dos registos a usar (0.1 = 10%%)")
    p.add_argument("--master", default="local[*]")
    p.add_argument("--driver-memory", default="6g")
    p.add_argument("--write-all", action="store_true",
                   help="gravar todos os resultados em parquet")
    p.add_argument("--resume", action="store_true",
                   help="saltar operacoes ja presentes no ficheiro de resultados")
    p.add_argument("--tag", default="",
                   help="sufixo do ficheiro de resultados (ex.: _s01)")
    p.add_argument("--flags", default=None,
                   help="para --variant any: lista prune,broadcast,cache,vectorized,shufflesize")
    return p.parse_args()


def build_spark(args, flags):
    b = (SparkSession.builder
         .master(args.master)
         .appName("compar-p1-" + args.variant)
         .config("spark.driver.memory", args.driver_memory)
         .config("spark.ui.showConsoleProgress", "false")
         # A auto-broadcast tem de estar desligada na baseline para que o
         # join com a tabela de zonas seja mesmo um SortMergeJoin; nas
         # variantes otimizadas usamos o hint broadcast explicito.
         .config("spark.sql.autoBroadcastJoinThreshold",
                 "-1" if not flags["broadcast"] else "10MB"))
    return b.getOrCreate()


def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def get_zones(spark, args, flags):
    zones = (spark.read.option("header", "true")
             .option("inferSchema", "true")
             .csv(args.zones))
    return sf.broadcast(zones) if flags["broadcast"] else zones


# ---------------------------------------------------------------------------
# M1 - ingestao e limpeza
# ---------------------------------------------------------------------------
def op_m1(spark, args, flags):
    reader = spark.read.parquet(args.input)
    raw = reader.select(*NEEDED_COLUMNS) if flags["prune"] else reader
    if args.sample < 1.0:
        raw = raw.sample(fraction=args.sample, seed=42)
    trips = (raw
             .withColumn("passenger_count", sf.col("passenger_count").cast("int"))
             .withColumn("trip_distance", sf.col("trip_distance").cast("double"))
             .withColumn("fare_amount", sf.col("fare_amount").cast("double"))
             .withColumn("PU", sf.col("PULocationID").cast("int"))
             .withColumn("DO", sf.col("DOLocationID").cast("int")))

    criteria = {
        "fare_le_0": sf.col("fare_amount").isNull() | (sf.col("fare_amount") <= 0),
        "distance_le_0": sf.col("trip_distance").isNull() | (sf.col("trip_distance") <= 0),
        "passengers_invalid": sf.col("passenger_count").isNull()
                              | (sf.col("passenger_count") <= 0)
                              | (sf.col("passenger_count") > 6),
        "dropoff_before_pickup": sf.col("tpep_pickup_datetime").isNull()
                                 | sf.col("tpep_dropoff_datetime").isNull()
                                 | (sf.col("tpep_dropoff_datetime")
                                    <= sf.col("tpep_pickup_datetime")),
        "location_invalid": sf.col("PU").isNull() | sf.col("DO").isNull()
                            | (sf.col("PU") < 1) | (sf.col("PU") > 263)
                            | (sf.col("DO") < 1) | (sf.col("DO") > 263),
    }
    valid = None
    for cond in criteria.values():
        valid = cond if valid is None else (valid | cond)

    # um unico passo de agregacao calcula o total e os removidos por criterio
    agg_exprs = [sf.count("*").alias("total")]
    for name, cond in criteria.items():
        agg_exprs.append(sf.sum(sf.when(cond, 1).otherwise(0)).alias(name))
    rows = (trips
            .withColumn("year", sf.year("tpep_pickup_datetime"))
            .groupBy("year")
            .agg(*agg_exprs)
            .collect())
    removed, total = {}, 0
    for r in rows:
        total += r["total"]
        for name in criteria:
            removed[name] = removed.get(name, 0) + r[name]

    cleaned = trips.filter(~valid)
    kept_rows = (cleaned
                 .withColumn("year", sf.year("tpep_pickup_datetime"))
                 .groupBy("year")
                 .agg(sf.count("*").alias("kept"))
                 .collect())
    stats = {"total_records": total,
             "removed_per_criterion": removed,
             "kept_per_year": {r["year"]: r["kept"] for r in kept_rows}}
    if flags["cache"]:
        cleaned = cleaned.cache()
        cleaned.count()  # materializa o cache uma unica vez
    return cleaned, stats


# ---------------------------------------------------------------------------
# M2 - procura temporal por (pickup_borough, dia da semana, hora)
# ---------------------------------------------------------------------------
def build_m2_df(spark, args, flags, trips, zones):
    joined = trips.join(zones, trips["PU"] == zones["LocationID"]) \
        .withColumnRenamed("Borough", "pickup_borough") \
        .withColumn("dow", sf.dayofweek("tpep_pickup_datetime")) \
        .withColumn("hour", sf.hour("tpep_pickup_datetime")) \
        .withColumn("duration_min",
                    (sf.unix_timestamp("tpep_dropoff_datetime")
                     - sf.unix_timestamp("tpep_pickup_datetime")) / 60.0)
    return (joined
            .groupBy("pickup_borough", "dow", "hour")
            .agg(sf.count("*").alias("trip_count"),
                 sf.round(sf.avg("fare_amount"), 2).alias("mean_fare"),
                 sf.round(sf.avg("duration_min"), 2).alias("mean_duration_min"),
                 sf.round(sf.avg("trip_distance"), 2).alias("mean_distance")))


def op_m2(spark, args, flags, trips, zones):
    result = build_m2_df(spark, args, flags, trips, zones)
    if args.write_all:
        (result.write.mode("overwrite")
         .partitionBy("pickup_borough")
         .parquet(os.path.join(args.out, "m2_temporal_demand")))
        return {"rows_written": result.count()}
    rows = result.collect()
    return {"rows": len(rows)}


# ---------------------------------------------------------------------------
# M3 - estatisticas de zona com janelas
# ---------------------------------------------------------------------------
def op_m3(spark, args, flags, trips, zones):
    monthly = trips.withColumn("month", sf.date_format("tpep_pickup_datetime", "yyyy-MM"))
    zone_month = (monthly
                  .groupBy("PU", "month")
                  .agg(sf.count("*").alias("trips"),
                       sf.round(sf.avg("fare_amount"), 2).alias("mean_fare"),
                       sf.round(sf.stddev("fare_amount"), 2).alias("std_fare"),
                       sf.round(sf.avg("trip_distance"), 2).alias("mean_distance"),
                       sf.round(sf.stddev("trip_distance"), 2).alias("std_distance")))
    joined = (zone_month.join(zones, zone_month["PU"] == zones["LocationID"])
              .withColumnRenamed("Borough", "borough")
              .withColumnRenamed("Zone", "zone"))

    w_rank = Window.partitionBy("borough", "month").orderBy(sf.desc("trips"))
    w_borough = (Window.partitionBy("borough")
                 .orderBy("month").rowsBetween(-2, 0))
    w_roll = (Window.partitionBy("borough", "PU")
              .orderBy("month").rowsBetween(-2, 0))
    w_stats = Window.partitionBy("PU").orderBy("month").rowsBetween(-2, 0)

    result = (joined
              .withColumn("rank_in_borough", sf.row_number().over(w_rank))
              .withColumn("roll_share",
                          sf.sum("trips").over(w_roll) / sf.sum("trips").over(w_borough))
              .withColumn("roll_mean", sf.avg("trips").over(w_stats))
              .withColumn("roll_std", sf.stddev("trips").over(w_stats))
              .withColumn("spike_flag",
                          sf.when(sf.col("trips") > sf.col("roll_mean")
                                  + 1.5 * sf.col("roll_std"), True)
                          .otherwise(False)))
    if args.write_all:
        result.write.mode("overwrite").parquet(os.path.join(args.out, "m3_zone_stats"))
    n = result.count()
    flagged = result.filter("spike_flag").count()
    return {"rows": n, "spike_rows": flagged}


# ---------------------------------------------------------------------------
# O1 - anomalias no preco por milha
# ---------------------------------------------------------------------------
def build_o1_df(spark, args, flags, trips, zones):
    bucket = trips.withColumn("hour", sf.hour("tpep_pickup_datetime"))
    if flags["vectorized"]:
        priced = bucket.withColumn("fare_per_mile",
                                   sf.col("fare_amount") / sf.col("trip_distance"))
    else:
        @sf.udf(DoubleType())
        def fare_per_mile_udf(fare, dist):
            if fare is None or dist is None or dist <= 0:
                return None
            return fare / dist
        priced = bucket.withColumn("fare_per_mile",
                                   fare_per_mile_udf(sf.col("fare_amount"),
                                                     sf.col("trip_distance")))
    stats = (priced
             .groupBy("PU", "hour")
             .agg(sf.avg("fare_per_mile").alias("bucket_mean"),
                  sf.stddev("fare_per_mile").alias("bucket_std")))
    if flags["bstats"]:
        stats = sf.broadcast(stats)  # ~260 zonas x 24 horas: join pequeno
    joined = (priced.join(stats, on=["PU", "hour"])
              .withColumn("is_anomaly",
                          sf.col("fare_per_mile") > sf.col("bucket_mean")
                          + 3 * sf.col("bucket_std"))
              .withColumn("weekend", sf.dayofweek("tpep_pickup_datetime").isin([1, 7]))
              .join(zones, sf.col("PU") == zones["LocationID"])
              .withColumnRenamed("Borough", "pickup_borough")
              .withColumn("payment_label",
                          sf.coalesce(*[sf.when(sf.col("payment_type") == k, v)
                                        for k, v in PAYMENT_LABELS.items()])))
    return (joined
            .groupBy("weekend", "payment_label", "pickup_borough")
            .agg(sf.round(sf.avg(sf.col("is_anomaly").cast("double")), 4)
                 .alias("anomaly_rate")))


def op_o1(spark, args, flags, trips, zones):
    rates = build_o1_df(spark, args, flags, trips, zones)
    if args.write_all:
        rates.write.mode("overwrite").parquet(os.path.join(args.out, "o1_anomaly_rates"))
    rows = rates.collect()
    return {"rate_rows": len(rows)}


# ---------------------------------------------------------------------------
# O2 - fluxos origem-destino
# ---------------------------------------------------------------------------
def op_o2(spark, args, flags, trips, zones):
    monthly = trips.withColumn("month", sf.date_format("tpep_pickup_datetime", "yyyy-MM"))
    pairs = (monthly
             .groupBy("PU", "DO", "month")
             .agg(sf.count("*").alias("trips")))
    w_share = Window.partitionBy("PU", "month")
    flows = (pairs
             .withColumn("share", sf.col("trips") / sf.sum("trips").over(w_share))
             .join(zones.alias("zpu"), sf.col("PU") == sf.col("zpu.LocationID"))
             .withColumnRenamed("Borough", "pu_borough")
             .withColumnRenamed("Zone", "pu_zone"))
    # Nota: um persist() intermediario aqui foi experimentado e medido
    # (piorou de 22s para 74s, 5815 tasks); ver discussao no relatorio.
    w_top = Window.partitionBy("pu_borough", "month").orderBy(sf.desc("trips"))
    flows = flows.withColumn("rank", sf.row_number().over(w_top))

    top20 = flows.filter("rank <= 20")
    top20_rows = top20.count()

    quarter = (flows
               .withColumn("q", sf.when(sf.col("month") <= "2024-03", "q1")
                           .otherwise(sf.when(sf.col("month") >= "2024-07", "q3")))
               .groupBy("PU", "DO", "q")
               .agg(sf.avg("share").alias("q_share")))
    q1 = quarter.filter("q = 'q1'").select("PU", "DO", sf.col("q_share").alias("s1"))
    q3 = quarter.filter("q = 'q3'").select("PU", "DO", sf.col("q_share").alias("s3"))
    qs = q1.join(q3, on=["PU", "DO"])  # s1, s3 por par (PU, DO)
    paired = (qs.alias("a")
              .join(qs.alias("b"),
                    (sf.col("a.PU") == sf.col("b.DO")) & (sf.col("a.DO") == sf.col("b.PU")),
                    "inner")
              .select(sf.col("a.PU").alias("PU"), sf.col("a.DO").alias("DO"),
                      sf.col("a.s1").alias("s1"), sf.col("a.s3").alias("s3"),
                      sf.col("b.s1").alias("rev_s1"), sf.col("b.s3").alias("rev_s3")))
    reversals = paired.filter(
        ((sf.col("s1") > sf.col("rev_s1")) & (sf.col("s3") < sf.col("rev_s3")))
        | ((sf.col("s1") < sf.col("rev_s1")) & (sf.col("s3") > sf.col("rev_s3"))))
    if args.write_all:
        (reversals.write.mode("overwrite")
         .parquet(os.path.join(args.out, "o2_direction_reversals")))
    return {"top20_rows_per_borough_month": top20_rows,
            "direction_reversals": reversals.count()}


# ---------------------------------------------------------------------------
# Medicao de assimetria (skew) das tasks, via REST API do Spark UI
# ---------------------------------------------------------------------------
def list_stages(spark, profiler):
    """Devolve {stageId: attemptId} de todos os estagios visiveis na Spark UI."""
    sc = spark.sparkContext
    all_stages = json.loads(urllib.request.urlopen(
        profiler.ui_base_url
        + f"/api/v1/applications/{sc.applicationId}/stages", timeout=10).read())
    return {s["stageId"]: s.get("attemptId", 0) for s in all_stages}


def skew_probe(spark, profiler, before):
    """Quantis do executorRunTime das tasks dos estagios novos desde `before`."""
    sc = spark.sparkContext
    out = []
    try:
        after = list_stages(spark, profiler)
        for sid, att in after.items():
            if sid in before:
                continue
            url = (profiler.ui_base_url
                   + f"/api/v1/applications/{sc.applicationId}/stages/{sid}"
                   + f"/{att}/taskSummary")
            try:
                with urllib.request.urlopen(url, timeout=10) as r:
                    d = json.loads(r.read().decode())
            except Exception as e:
                out.append({"stage_id": sid, "error": repr(e)})
                continue
            rt = d.get("executorRunTime", [])
            if rt:
                out.append({"stage_id": sid,
                            "median_ms": rt[0], "p75_ms": rt[1],
                            "p95_ms": rt[2], "max_ms": rt[4]})
    except Exception as e:
        out.append({"error": repr(e)})
    return out


def main():
    args = parse_args()
    if args.variant == "any":
        names = args.flags.split(",")
        flags = {k: (k in names) for k in VARIANTS["final"]}
    else:
        flags = VARIANTS[args.variant]

    ops = [o.strip() for o in args.ops.split(",") if o.strip()]
    results_path = os.path.join(args.results, args.variant + args.tag + ".json")
    os.makedirs(args.results, exist_ok=True)
    if os.path.exists(results_path):
        with open(results_path, encoding="utf-8") as f:
            results = json.load(f)
    else:
        results = {"variant": args.variant, "sample": args.sample,
                   "master": args.master, "flags": flags, "ops": {}}
    if args.resume:
        done = set(results["ops"].keys())
        ops = [o for o in ops if o not in done]
        if not ops:
            print("nada a fazer (todas as operacoes ja medidas)")
            return

    spark = build_spark(args, flags)
    profiler = SparkProfiler(spark)
    total_start = time.time()

    try:
        zones = get_zones(spark, args, flags)
        trips = None
        for op in ops:
            # se a corrida nao incluiu o m1, carrega e limpa na mesma
            # (sem medir) para as restantes operacoes poderem correr
            if trips is None and op != "m1":
                spark.sparkContext.setJobGroup("warmup-" + str(time.time()), "warmup")
                trips, _ = op_m1(spark, args, flags)
            print(f"--- a correr {op} (variante {args.variant}) ---", flush=True)
            stages_before = list_stages(spark, profiler)
            with profiler.profile(op):
                if op == "m1":
                    trips, extra = op_m1(spark, args, flags)
                elif op == "m2":
                    extra = op_m2(spark, args, flags, trips, zones)
                elif op == "m3":
                    extra = op_m3(spark, args, flags, trips, zones)
                elif op == "o1":
                    extra = op_o1(spark, args, flags, trips, zones)
                elif op == "o2":
                    extra = op_o2(spark, args, flags, trips, zones)
                else:
                    raise ValueError("operacao desconhecida: " + op)
            entry = dict(profiler.runs[op])
            entry.update(extra or {})
            if op == "o1" and not flags["vectorized"]:
                entry["task_skew"] = skew_probe(spark, profiler, stages_before)
            results["ops"][op] = entry
            save_json(results_path, results)
            print(f"[{op}] {entry['wall_time_s']}s "
                  f"shuffle_read={entry['shuffle_read_bytes']} "
                  f"shuffle_write={entry['shuffle_write_bytes']} "
                  f"tasks={entry['num_tasks']} input={entry['input_bytes']}",
                  flush=True)
        results["total_wall_s"] = round(time.time() - total_start, 2)
        save_json(results_path, results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
