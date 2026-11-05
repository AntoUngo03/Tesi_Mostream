/* Atomic primitives for the native Mojo wCQ port. No queue algorithm here.
 * Linux x86-64, little endian, CMPXCHG16B. All operations are sequentially
 * consistent. Pair layout: addon at byte 0, entry/counter at byte 8.
 * SPDX-License-Identifier: MIT
 */
#include <stdint.h>
#include <stdlib.h>
#include <cpuid.h>

#if !defined(__x86_64__) || __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
#error "wcq_atomics requires little-endian x86-64"
#endif

uint64_t mostream_wcq_alloc(uint64_t bytes) {
    unsigned a, b, c, d;
    if (!__get_cpuid(1, &a, &b, &c, &d) || !(c & bit_CMPXCHG16B)) return 0;
    void *p = NULL;
    if (!bytes || posix_memalign(&p, 64, bytes)) return 0;
    return (uintptr_t)p;
}

void mostream_wcq_free(uint64_t address) { free((void *)(uintptr_t)address); }

uint64_t mostream_wcq_load(uint64_t address) {
    return __atomic_load_n((uint64_t *)(uintptr_t)address, __ATOMIC_SEQ_CST);
}
void mostream_wcq_store(uint64_t address, uint64_t value) {
    __atomic_store_n((uint64_t *)(uintptr_t)address, value, __ATOMIC_SEQ_CST);
}
uint64_t mostream_wcq_add(uint64_t address, uint64_t value) {
    return __atomic_fetch_add((uint64_t *)(uintptr_t)address, value, __ATOMIC_SEQ_CST);
}
uint64_t mostream_wcq_or(uint64_t address, uint64_t value) {
    return __atomic_fetch_or((uint64_t *)(uintptr_t)address, value, __ATOMIC_SEQ_CST);
}
int32_t mostream_wcq_cas(uint64_t address, uint64_t expected, uint64_t desired) {
    return __atomic_compare_exchange_n((uint64_t *)(uintptr_t)address,
        &expected, desired, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

/* Use the instruction directly: a libatomic implementation may use a lock.
 * Memory operands cover both words; "memory" also supplies a compiler fence.
 */
int32_t mostream_wcq_cas2(uint64_t address, uint64_t entry, uint64_t addon,
                        uint64_t new_entry, uint64_t new_addon) {
    unsigned char success;
    __asm__ volatile("lock cmpxchg16b %1; sete %0"
        : "=q"(success), "+m"(*(unsigned __int128 *)(uintptr_t)address),
          "+a"(addon), "+d"(entry)
        : "b"(new_addon), "c"(new_entry) : "cc", "memory");
    return success;
}

void mostream_wcq_load2(uint64_t address, uint64_t *entry, uint64_t *addon) {
    uint64_t lo = 0, hi = 0;
    __asm__ volatile("lock cmpxchg16b %0"
        : "+m"(*(unsigned __int128 *)(uintptr_t)address), "+a"(lo), "+d"(hi)
        : "b"((uint64_t)0), "c"((uint64_t)0) : "cc", "memory");
    *entry = hi;
    *addon = lo;
}
