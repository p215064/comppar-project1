"""
run_plans.py
------------
Captura o plano fisico (explain mode="formatted") de operacoes escolhidas,
sem executar o job completo. Grava em results/plans/<nome>.txt para anexar
ao relatorio.
"""

import argparse
import contextlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
import pipeline as P


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data")
    ap.add_argument("--zones", default="data/zones/taxi_zone_lookup.csv")
    ap.add_argument("--results", default="results")
    ap.add_argument("--which", default="m2_baseline,m2_final,o1_baseline,o1_final")
    ap.add_argument("--sample", type=float, default=1.0)
    args = ap.parse_args()

    os.makedirs(os.path.join(args.results, "plans"), exist_ok=True)

    class A:  # namespace minimo para build_spark / op_m2 / op_o1
        pass

    for which in [w.strip() for w in args.which.split(",")]:
        op, variant = which.split("_")
        # cache forçado só para não repetir a leitura durante a captura;
        # não afeta o plano do join em si
        flags = dict(P.VARIANTS[variant], cache=True)
        a = A()
        a.master, a.variant, a.driver_memory = "local[*]", variant, "6g"
        a.input = args.input
        a.sample = args.sample
        spark = P.build_spark(a, flags)
        try:
            trips, _ = P.op_m1(spark, a, flags)
            zones = P.get_zones(spark, args, flags)
            if op == "m2":
                result = P.build_m2_df(spark, args, flags, trips, zones)
            else:
                result = P.build_o1_df(spark, args, flags, trips, zones)
            path = os.path.join(args.results, "plans", which + ".txt")
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"### plano fisico de {op} na variante {variant} ###\n")
                with contextlib.redirect_stdout(f):
                    result.explain(mode="formatted")
            print("gravado:", path)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
