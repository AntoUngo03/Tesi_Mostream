"""Grafici del confronto MPMC; include tutte le configurazioni misurate."""
import argparse
import csv
from pathlib import Path


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results',type=Path)
    args=parser.parse_args()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    rows=list(csv.DictReader((args.results/'summary.csv').open()))
    targets=sorted({float(r['target_us']) for r in rows})
    capacities=sorted({int(r['capacity']) for r in rows})
    pins=sorted({r['pinning'] for r in rows})
    combinations=[(c,p) for c in capacities for p in pins]
    colors={'vyukov':'#d97706','rigtorp':'#2563eb'}
    fig,axes=plt.subplots(len(combinations),len(targets),figsize=(15,3.4*len(combinations)),squeeze=False)
    for i,(capacity,pin) in enumerate(combinations):
        for j,target in enumerate(targets):
            ax=axes[i,j]
            group=sorted((r for r in rows if int(r['capacity'])==capacity and r['pinning']==pin and float(r['target_us'])==target),key=lambda r:int(r['workers']))
            n=[int(r['workers']) for r in group]
            for b in colors:
                ax.plot(n,[float(r[b+'_median'])/1e6 for r in group],marker='o',color=colors[b],label=b.capitalize())
                ax.fill_between(n,[float(r[b+'_min'])/1e6 for r in group],[float(r[b+'_max'])/1e6 for r in group],color=colors[b],alpha=.13)
            ax.set_title(f'T={target:g} µs · capacità {capacity} · pinning {pin}')
            ax.set_xscale('log',base=2); ax.set_xticks(n,[str(v) for v in n]); ax.set_ylim(bottom=0)
            ax.set_xlabel('N repliche per stadio (2N+2 task)'); ax.set_ylabel('Milioni di input/s')
            ax.grid(axis='y',alpha=.25); ax.spines[['top','right']].set_visible(False)
            if i==0 and j==0: ax.legend(frameon=False)
    fig.suptitle('MoStream: Source → N Compute A → N Compute B → Sink',fontsize=17)
    fig.text(.5,.005,'Mediane del throughput · Fasce min–max, non intervalli di confidenza · T: lavoro totale per input',ha='center')
    fig.tight_layout(rect=(0,.025,1,.965))
    for ext in ('png','pdf','svg'): fig.savefig(args.results/f'mpmc_throughput.{ext}',dpi=180,facecolor='white')
    plt.close(fig)
    fig,axes=plt.subplots(len(capacities),len(pins),figsize=(12,4*len(capacities)),squeeze=False,sharey=True)
    for i,capacity in enumerate(capacities):
        for j,pin in enumerate(pins):
            ax=axes[i,j]
            for target in targets:
                group=sorted((r for r in rows if int(r['capacity'])==capacity and r['pinning']==pin and float(r['target_us'])==target),key=lambda r:int(r['workers']))
                n=[int(r['workers']) for r in group]
                ax.plot(n,[(float(r['rigtorp_over_vyukov'])-1)*100 for r in group],marker='o',label=f'T={target:g} µs')
            ax.axhline(0,color='#374151',linewidth=1)
            ax.set_xscale('log',base=2); ax.set_xticks(n,[str(v) for v in n])
            ax.set_title(f'Capacità {capacity} · pinning {pin}'); ax.set_xlabel('N repliche per stadio')
            ax.set_ylabel('Differenza throughput Rigtorp (%)'); ax.grid(alpha=.2); ax.legend(frameon=False)
    fig.suptitle('Sopra zero: mediana Rigtorp maggiore · Sotto zero: mediana Vyukov maggiore',fontsize=13)
    fig.tight_layout(rect=(0,0,1,.94))
    for ext in ('png','pdf','svg'): fig.savefig(args.results/f'mpmc_relative.{ext}',dpi=180,facecolor='white')
    plt.close(fig)


if __name__=='__main__': main()
