"""Gera as figuras do relatorio a partir de results/resumo.json."""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(HERE, "..", "results", "resumo.json"), encoding="utf-8") as f:
    R = json.load(f)

FRACS = R["escala"]["fracoes"]
OPS = ["m1", "m2", "m3", "o1", "o2"]
LABELS = {"m1": "M1", "m2": "M2", "m3": "M3", "o1": "O1", "o2": "O2"}

fig, ax = plt.subplots(figsize=(5.6, 3.2))
markers = {"m1": "o", "m2": "s", "m3": "^", "o1": "D", "o2": "v"}
for op in OPS:
    ax.plot(FRACS, R["escala"][op], marker=markers[op], markersize=5,
            linewidth=1.3, label=LABELS[op])
totals = [sum(R["escala"][op][i] for op in OPS) for i in range(3)]
ax.plot(FRACS, totals, marker="*", markersize=8, linewidth=2.0, color="black",
        label="total")
ax.set_xscale("log")
ax.set_xticks(FRACS)
ax.set_xticklabels(["10%", "20%", "100%"])
ax.set_xlabel("Fracao da amostra")
ax.set_ylabel("Tempo de execucao (s)")
ax.grid(True, which="both", alpha=0.3)
ax.legend(ncol=3, fontsize=8, frameon=False)
fig.tight_layout()
fig.savefig(os.path.join(HERE, "..", "report", "fig_scale.pdf"))
plt.close(fig)

c = R["cores"]
sp_m2 = c["m2_4t_s"] / c["m2_16t_s"]
sp_o2 = c["o2_4t_s"] / c["o2_16t_s"]

fig, ax = plt.subplots(figsize=(3.6, 2.8))
xs = [0, 1, 2]
names = ["M2", "O2", "O2 (ideal)"]
vals = [sp_m2, sp_o2, 4.0]
ax.bar(xs, vals, color=["#4878cf", "#4878cf", "#bbbbbb"], width=0.55)
for x, v in zip(xs, vals):
    ax.text(x, v + 0.08, f"{v:.2f}x", ha="center", fontsize=9)
ax.set_xticks(xs)
ax.set_xticklabels(names, fontsize=9)
ax.set_ylabel("Speedup (16 threads vs 4)")
ax.set_ylim(0, 4.6)
ax.grid(True, axis="y", alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(HERE, "..", "report", "fig_speedup.pdf"))
plt.close(fig)

print("speedups:", round(sp_m2, 2), round(sp_o2, 2))
print("figs gravadas")
