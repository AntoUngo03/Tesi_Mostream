"""Confronto esplorativo MPMC: due stadi paralleli, pinning on/off, stessi input."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile


def tag(output, name):
    return next(line.split()[1:] for line in output.splitlines() if line.startswith(name + ' '))


def physical_cpus():
    selected = {}
    for cpu in sorted(os.sched_getaffinity(0)):
        topology = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
        key = (int((topology/'physical_package_id').read_text()), int((topology/'core_id').read_text()))
        selected.setdefault(key, cpu)
    return [selected[key] for key in sorted(selected)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mojo', default='mojo')
    parser.add_argument('--messages', type=int, default=50000)
    parser.add_argument('--workers', default='1,2,4,8')
    parser.add_argument('--work-us', default='0,0.5,2')
    parser.add_argument('--capacities', default='64,1024')
    parser.add_argument('--pinning', default='off,on')
    parser.add_argument('--repeats', type=int, default=7)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    here = Path(__file__).resolve().parent
    workers = sorted({int(v) for v in args.workers.split(',')})
    targets = sorted({float(v) for v in args.work_us.split(',')})
    capacities = sorted({int(v) for v in args.capacities.split(',')})
    pins = args.pinning.split(',')
    if args.messages < 1 or args.repeats < 1 or min(workers) < 1 or min(targets) < 0:
        parser.error('Parametri non validi')
    if any(c < 2 or c & (c-1) for c in capacities) or not set(pins) <= {'on','off'}:
        parser.error('Capacità potenza di due >=2; pinning on/off')
    cpus = physical_cpus()
    if 'on' in pins and len(cpus) < 2*max(workers)+2:
        parser.error(f'Servono {2*max(workers)+2} core fisici distinti, disponibili {len(cpus)}')
    args.output.mkdir(parents=True, exist_ok=False)
    environment = {**os.environ, 'MOSTREAM_HOME': str(root), 'MOSTREAM_PINNING': ','.join(map(str,cpus))}

    def execute(command, timeout=180):
        output = subprocess.check_output(command, cwd=root, env=environment, text=True, stderr=subprocess.STDOUT, timeout=timeout)
        if 'failed to pin' in output or 'Warning' in output:
            raise RuntimeError(output)
        return output

    # Helper usato dalla libreria stessa; nessuna modifica al suo algoritmo.
    subprocess.run(['gcc','-D_GNU_SOURCE','-shared','-fPIC','-O2','-pthread',str(root/'MoStream/lib/libpinning.c'),
                    '-o',str(root/'MoStream/lib/libpinning.so')],check=True)
    source_paths = [*sorted((root/'MoStream').rglob('*.mojo')), root/'MoStream/lib/libpinning.c', here/'farm_mpmc.mojo',Path(__file__)]
    metadata = dict(utc=datetime.now(timezone.utc).isoformat(), host=platform.node(),
                    mojo=execute([args.mojo,'--version']).strip(), cpu=Path('/proc/cpuinfo').read_text(),
                    affinity=sorted(os.sched_getaffinity(0)), physical_cpu_order=cpus,
                    parameters={**vars(args),'output':str(args.output)},seed=20261027,
                    topology='source -> N compute A -> N compute B -> sink; 2*N+2 tasks',
                    scope='Pipeline.run wall time, includes pinning/setup/EOS/teardown; exact validation outside timer',
                    sha256={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths})
    rows=[]
    with tempfile.TemporaryDirectory(prefix='mostream-mpmc-') as temp:
        binaries={}
        for backend in ('vyukov','rigtorp'):
            path=Path(temp)/backend
            command=[args.mojo,'build','-I',str(root)]
            if backend=='rigtorp': command+=['-D','MOSTREAM_RIGTORP=1']
            subprocess.run([*command,str(here/'farm_mpmc.mojo'),'-o',str(path)],cwd=root,check=True,timeout=240)
            binaries[backend]=str(path)
        available=int(tag(execute([binaries['vyukov'],'info']),'WORKERS')[0])
        if 2*max(workers)+2 > available: parser.error(f'Solo {available} worker runtime')
        samples=[float(tag(execute([binaries['vyukov'],'calibrate']),'CALIBRATION')[0]) for _ in range(5)]
        ns_iter=statistics.median(samples)
        iterations={t:0 if t==0 else max(1,round(t*1000/ns_iter)) for t in targets}
        metadata.update(runtime_workers=available,calibration_ns=samples,ns_per_iteration=ns_iter)
        (args.output/'metadata.json').write_text(json.dumps(metadata,indent=2))
        for backend,pin in itertools.product(binaries,pins):
            execute([binaries[backend],'1000',str(max(workers)),'0',str(min(capacities)),pin])
        jobs=list(itertools.product(workers,targets,capacities,pins,range(args.repeats)))
        rng=random.Random(metadata['seed']); rng.shuffle(jobs)
        with (args.output/'raw.csv').open('w') as stream:
            writer=None
            for index,(n,t,capacity,pin,repeat) in enumerate(jobs):
                backends=list(binaries); rng.shuffle(backends)
                for backend in backends:
                    elapsed=int(tag(execute([binaries[backend],str(args.messages),str(n),str(iterations[t]),str(capacity),pin]),'RESULT')[0])
                    row=dict(backend=backend,workers=n,target_us=t,capacity=capacity,pinning=pin,repeat=repeat,
                             iterations=iterations[t],messages=args.messages,elapsed_ns=elapsed,throughput=args.messages*1e9/elapsed)
                    if writer is None: writer=csv.DictWriter(stream,fieldnames=row); writer.writeheader()
                    writer.writerow(row); stream.flush(); rows.append(row)
                print(f'{index+1}/{len(jobs)} N={n} T={t} capacity={capacity} pin={pin}',flush=True)
    summary=[]
    for n,t,c,pin in itertools.product(workers,targets,capacities,pins):
        group={b:[r for r in rows if (r['backend'],r['workers'],r['target_us'],r['capacity'],r['pinning'])==(b,n,t,c,pin)] for b in binaries}
        med={b:statistics.median(r['throughput'] for r in g) for b,g in group.items()}
        paired={b:{r['repeat']:r['throughput'] for r in g} for b,g in group.items()}
        ratios=[paired['rigtorp'][i]/paired['vyukov'][i] for i in range(args.repeats)]
        row=dict(workers=n,target_us=t,capacity=c,pinning=pin,repeats=args.repeats,
                 vyukov_median=med['vyukov'],rigtorp_median=med['rigtorp'],rigtorp_over_vyukov=med['rigtorp']/med['vyukov'],
                 paired_ratio_median=statistics.median(ratios),rigtorp_pair_wins=sum(r>1 for r in ratios))
        for b,g in group.items():
            row[b+'_min']=min(r['throughput'] for r in g); row[b+'_max']=max(r['throughput'] for r in g)
        summary.append(row)
    with (args.output/'summary.csv').open('w') as stream:
        writer=csv.DictWriter(stream,fieldnames=summary[0]); writer.writeheader(); writer.writerows(summary)
    print('PASS',len(rows),'esecuzioni con verifica esatta; nessun caso omesso')
    for row in sorted(summary,key=lambda r:r['rigtorp_over_vyukov'],reverse=True)[:5]: print(row)


if __name__=='__main__': main()
