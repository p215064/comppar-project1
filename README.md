# ComPar 26/27 — Project 1: Large-Scale Analytics with Spark

Pipeline PySpark de analise de desempenho sobre os Yellow Taxi Trip
Records da NYC TLC (ano de 2024), com M1, M2, M3 e as operacoes
opcionais O1 e O2. Autor: Rodrigo Mira (79432), trabalho individual.

## Estrutura

```
project1/
  data/                  ficheiros de dados (NAO versionados)
    yellow_tripdata_2024-*.parquet   12 ficheiros mensais
    zones/taxi_zone_lookup.csv       tabela de zonas (265 linhas)
  src/
    pipeline.py          pipeline principal (M1-M3, O1, O2) com CLI
    spark_profiler.py    profiler por estagio via REST API da Spark UI
                         (adaptado do Lab 3; grava JSON por operacao)
    run_plans.py         captura planos fisicos (explain) para results/plans/
    scan_probe.py        sondagem A/B da poda de colunas
    make_figs.py         gera as figuras do relatorio
  results/               metricas de todas as corridas (JSON)
    plans/               planos fisicos capturados
    resumo.json          numeros consolidados usados no relatorio
  output/                resultados Parquet das operacoes
  report/                relatorio LaTeX (template acmart) + figuras
  tools/winutils/        winutils.exe + hadoop.dll para Windows
```

## Dados

Nao sao versionados. Download (acedido em novembro de 2026):

* Viagens: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-MM.parquet`
  para MM de 01 a 12, colocados em `data/`
* Zonas: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
  colocado em `data/zones/taxi_zone_lookup.csv`

Origem oficial dos dados: NYC Taxi & Limousine Commission,
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
(ano 2024, ~41,2 milhoes de registos, ~700 MB comprimido).

## Ambiente

* Windows 11, Java 21 (Eclipse Adoptium), PySpark 4.2.0, Python 3.12.
* Modo local: `local[*]` (8 nucleos fisicos / 16 threads, 16 GB RAM,
  driver com 6 GB).
* No Windows e preciso o winutils compativel com Hadoop 3.4. A pasta
  `tools/winutils/bin` tem de estar no `PATH` (o PySpark constroi o
  `java.library.path` a partir do `PATH`, nao do `HADOOP_HOME`). O
  script `run.sh` faz isto; no PowerShell/cmd faca o equivalente.

## Como correr

A partir da raiz do projeto. Em Git Bash:

```bash
bash run.sh --variant baseline --ops m1,m2,m3,o1,o2
bash run.sh --variant final    --ops m1,m2,m3,o1,o2
```

ou diretamente com as variaveis de ambiente definidas (ver `run.sh`):

```bash
python src/pipeline.py --variant final --ops m1,m2,m3,o1,o2
```

Parametros principais de `pipeline.py`:

* `--variant`: `baseline` (tudo desligado), `final` (tudo ligado) ou
  `any` com `--flags prune,broadcast,cache,vectorized,bstats`.
  Variantes de controlo: `noprun`, `nobroadcast`, `nocache`,
  `novector`, `nobstats` (final com uma otimizacao desligada).
* `--ops`: subconjunto de `m1,m2,m3,o1,o2` (util para isolar efeitos).
* `--input`: pasta ou ficheiro(s) Parquet (por omissao `data`).
* `--sample FRAC`: fracao de amostragem (ex.: 0.1).
* `--master`: por omissao `local[*]`; usar `local[4]` para o estudo
  de escalonamento.
* `--write-all`: escreve os resultados em `output/`.
* `--tag SUF`: sufixo do ficheiro de resultados (ex.: `_s01`).
* `--resume`: retoma corridas interrompidas.

Cada corrida grava `results/<variante><tag>.json` com, por operacao:
tempo de parede, numero de estagios/tasks, bytes de shuffle,
tempos de execucao/CPU dos executores e contagens de negocio.

## Reproducao do estudo

```bash
# baseline e final completos (ano inteiro)
bash run.sh --variant baseline --ops m1
bash run.sh --variant baseline --ops m2
bash run.sh --variant baseline --ops m3
bash run.sh --variant baseline --ops o1 --sample 0.1
bash run.sh --variant baseline --ops o2

bash run.sh --variant final --ops m1,m2        # sessao para o efeito do cache
bash run.sh --variant nocache --ops m1,m2      # controlo do cache
bash run.sh --variant nobroadcast --ops m1,m2  # controlo do broadcast
bash run.sh --variant final --ops m3,o1,o2

# A/B da UDF (amostra de 10%)
bash run.sh --variant novector --ops o1 --sample 0.1
bash run.sh --variant final --ops o1 --sample 0.1 --tag _s01

# controlo do broadcast das estatisticas do O1
bash run.sh --variant nobstats --ops o1

# sondagem da poda de colunas
python src/scan_probe.py

# escalabilidade: 10%, 20%, e numero de cores
bash run.sh --variant final --ops m1 --sample 0.1 --tag _s01
bash run.sh --variant final --ops m2 --sample 0.1 --tag _s01
bash run.sh --variant final --ops m3 --sample 0.1 --tag _s01
bash run.sh --variant final --ops o2 --sample 0.1 --tag _s01
bash run.sh --variant final --ops m1 --sample 0.2 --tag _s02
bash run.sh --variant final --ops m2 --sample 0.2 --tag _s02
bash run.sh --variant final --ops m3 --sample 0.2 --tag _s02
bash run.sh --variant final --ops o1 --sample 0.2 --tag _s02
bash run.sh --variant final --ops o2 --sample 0.2 --tag _s02
bash run.sh --variant final --ops m2 --master local[4] --tag _c4
bash run.sh --variant final --ops o2 --master local[4] --tag _c4

# planos fisicos e figuras
python src/run_plans.py --input data
python src/make_figs.py
```

Nota: as corridas tem ruido de 10 a 20% entre processos; as
comparacoes A/B devem ser feitas em processos novos e emparelhados,
como acima.

## Resultados principais (ver report/)

* Baseline (ano inteiro, O1 a 10%): 91 s. Final: 56 s no ano inteiro.
* Broadcast do zone lookup: shuffle do M2 de 797.5 MB para 0.7 MB.
* UDF -> operador nativo no O1: 37.7 s -> 2.7 s (amostra de 10%).
* Relatorio: `report/report.pdf` (compilar com `tectonic -X compile
  report/report.tex` ou qualquer distribuicao LaTeX; o template acmart
  esta incluido).
