# Vendored wCQ source

The files in this directory are unmodified copies from:

- Repository: <https://github.com/rusnikola/lfqueue>
- Commit: `708c0052872950dcb15b487fa7a5dd77ce2a2746`
- Upstream file: `wfring_cas2.h` and its `lf/` dependencies
- Retrieved for this project: 2026-07-22

The upstream source identifies wCQ as the implementation accompanying
“wCQ: A Fast Wait-Free Queue with Bounded Memory Usage” (SPAA 2022).

The source is dual-licensed under BSD-2-Clause and MIT. The complete upstream
copyright and both license grants are retained verbatim at the top of every
vendored source file.

SHA-256 checksums of the vendored files at the commit above:

```
cc7c568d58de06cc641d4eaa77f484c7ad7544729d5133a2e3f4c4e98aca79f0  wfring_cas2.h
f508b7c84d77427e97549b43c41082655533182b11f91ec28f5ab896a67b5e47  lf/lf.h
688e6eb7282249a9617debd12589d19bb0dbcab99aa2f3e7984b47823fa6fb98  lf/config.h
f0c30fc3acb592406b13131c9a90ed319182f55c9c187851744c4e940ed0f3db  lf/gcc_x86.h
6af164f13496e203205acd1b8805508770ed22ca8c70eddcd5b0bc18f8968d9b  lf/c11.h
```

`../wcq_native.c` is a MoStream adapter. It uses two wCQ rings and a fixed
payload array so that every `uint64_t` bit pattern is valid data while the
public capacity remains bounded.

The adapter also maintains two padded admission counters. `free_count` counts
free indices that are already visible in the free-index ring; `item_count`
counts items that are already visible in the available-item ring. A try
operation performs at most one strong admission CAS. On CAS contention it
returns `WOULD_BLOCK`, even when the abstract queue may have room/data. Only a
successfully reserved token is dequeued with upstream's `nonempty=true` mode.
The corresponding counter is incremented only after the index is published to
the other ring. Consequently, the two counters need not sum to the capacity
while operations are in flight.

The blocking convenience functions retry and are not wait-free. The try path
is deliberately bounded and uses the upstream wait-free rings, but this
two-ring/admission composition has not been accompanied by a separate formal
wait-freedom or linearizability proof in this project; those properties should
not be claimed solely from the stress-test results. In particular, a thread
paused after decrementing an admission counter temporarily owns that token;
until it resumes, another try may report `WOULD_BLOCK` and a blocking wrapper
may wait even though the corresponding ring operation has not yet happened.
