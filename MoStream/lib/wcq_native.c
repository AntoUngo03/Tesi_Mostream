/*
 * Native bounded uint64_t adapter for wCQ.
 *
 * The underlying wCQ implementation is the upstream SPAA'22 artifact at
 * commit 708c0052872950dcb15b487fa7a5dd77ce2a2746. Its dual BSD-2-Clause/MIT
 * license and copyright notice are retained in wcq_vendor/wfring_cas2.h and
 * the wcq_vendor/lf headers. See wcq_vendor/UPSTREAM.md for provenance.
 */

#define _POSIX_C_SOURCE 200112L

#include "wcq_native.h"

#include <cpuid.h>
#include <errno.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "wcq_vendor/wfring_cas2.h"

#if !defined(__x86_64__)
#error "The vendored CAS2 wCQ adapter is currently supported on x86-64 only."
#endif

struct mostream_wcq_u64 {
    size_t capacity;
    size_t max_threads;
    size_t order;
    struct wfring *available;
    struct wfring *free_indices;
    uint64_t *values;
    struct wfring_state *available_states;
    struct wfring_state *free_states;
    /*
     * Admission tokens are published only after their corresponding index is
     * present in a ring. A successful decrement therefore justifies calling
     * wfring_dequeue(..., nonempty=true), without speculative empty dequeues
     * advancing the ring head. They are isolated because both are contended.
     */
    _Alignas(LF_CACHE_BYTES) _Atomic size_t item_count;
    _Alignas(LF_CACHE_BYTES) _Atomic size_t free_count;
};

static bool mostream_is_power_of_two(size_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

static size_t mostream_log2_power_of_two(size_t value)
{
    size_t order = 0;
    while (value > 1) {
        value >>= 1;
        ++order;
    }
    return order;
}

static void *mostream_aligned_zero_alloc(size_t alignment, size_t size)
{
    void *memory = NULL;
    if (size == 0 || posix_memalign(&memory, alignment, size) != 0)
        return NULL;
    memset(memory, 0, size);
    return memory;
}

static void mostream_init_state_ring(
    struct wfring *ring,
    struct wfring_state *states,
    size_t count)
{
    size_t i;
    for (i = 0; i < count; ++i)
        wfring_init_state(ring, &states[i]);
    for (i = 0; i < count; ++i) {
        atomic_store_explicit(
            &states[i].next,
            &states[(i + 1) % count],
            memory_order_relaxed);
    }
}

int mostream_wcq_u64_create(
    size_t capacity,
    size_t max_threads,
    mostream_wcq_u64 **out_queue)
{
    mostream_wcq_u64 *queue;
    size_t minimum_capacity = wfring_pow2(WFRING_MIN);
    size_t order;
    size_t ring_size;

    if (out_queue == NULL)
        return EINVAL;
    *out_queue = NULL;

    if (!mostream_is_power_of_two(capacity) ||
        capacity < minimum_capacity ||
        max_threads == 0 || max_threads > capacity)
        return EINVAL;

    {
        unsigned int eax, ebx, ecx, edx;
        if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx) ||
            (ecx & bit_CMPXCHG16B) == 0)
            return ENOTSUP;
    }

    order = mostream_log2_power_of_two(capacity);
    if (order >= sizeof(size_t) * 8 - 1 ||
        capacity > SIZE_MAX / (2 * sizeof(lfatomic_big_t)) ||
        capacity > SIZE_MAX / sizeof(uint64_t) ||
        max_threads > SIZE_MAX / sizeof(struct wfring_state))
        return EOVERFLOW;
    ring_size = WFRING_SIZE(order);

    queue = mostream_aligned_zero_alloc(
        _Alignof(mostream_wcq_u64), sizeof(*queue));
    if (queue == NULL)
        return ENOMEM;

    queue->capacity = capacity;
    queue->max_threads = max_threads;
    queue->order = order;
    atomic_init(&queue->item_count, 0);
    atomic_init(&queue->free_count, capacity);
    queue->available = mostream_aligned_zero_alloc(WFRING_ALIGN, ring_size);
    queue->free_indices = mostream_aligned_zero_alloc(WFRING_ALIGN, ring_size);
    queue->values = calloc(capacity, sizeof(*queue->values));
    queue->available_states = mostream_aligned_zero_alloc(
        _Alignof(struct wfring_state),
        max_threads * sizeof(*queue->available_states));
    queue->free_states = mostream_aligned_zero_alloc(
        _Alignof(struct wfring_state),
        max_threads * sizeof(*queue->free_states));

    if (queue->available == NULL || queue->free_indices == NULL ||
        queue->values == NULL || queue->available_states == NULL ||
        queue->free_states == NULL) {
        mostream_wcq_u64_destroy(queue);
        return ENOMEM;
    }

    wfring_init_empty(queue->available, order);
    wfring_init_full(queue->free_indices, order);
    mostream_init_state_ring(
        queue->available, queue->available_states, max_threads);
    mostream_init_state_ring(
        queue->free_indices, queue->free_states, max_threads);

    *out_queue = queue;
    return 0;
}

void mostream_wcq_u64_destroy(mostream_wcq_u64 *queue)
{
    if (queue == NULL)
        return;
    free(queue->free_states);
    free(queue->available_states);
    free(queue->values);
    free(queue->free_indices);
    free(queue->available);
    free(queue);
}

size_t mostream_wcq_u64_capacity(const mostream_wcq_u64 *queue)
{
    return queue == NULL ? 0 : queue->capacity;
}

size_t mostream_wcq_u64_max_threads(const mostream_wcq_u64 *queue)
{
    return queue == NULL ? 0 : queue->max_threads;
}

static bool mostream_valid_thread(
    const mostream_wcq_u64 *queue,
    size_t thread_id)
{
    return queue != NULL && thread_id < queue->max_threads;
}

int mostream_wcq_u64_try_enqueue(
    mostream_wcq_u64 *queue,
    size_t thread_id,
    uint64_t value)
{
    size_t free_count;
    size_t index;

    if (!mostream_valid_thread(queue, thread_id))
        return MOSTREAM_WCQ_ERROR;

    free_count = atomic_load_explicit(&queue->free_count, memory_order_acquire);
    if (free_count == 0)
        return MOSTREAM_WCQ_WOULD_BLOCK;
    /* One CAS only: contention is reported as a transient WOULD_BLOCK. */
    if (!atomic_compare_exchange_strong_explicit(
            &queue->free_count,
            &free_count,
            free_count - 1,
            memory_order_acq_rel,
            memory_order_acquire))
        return MOSTREAM_WCQ_WOULD_BLOCK;

    index = wfring_dequeue(
        queue->free_indices,
        queue->order,
        true,
        &queue->free_states[thread_id]);
    if (index == WFRING_EMPTY || index >= queue->capacity) {
        atomic_fetch_add_explicit(
            &queue->free_count, 1, memory_order_release);
        return MOSTREAM_WCQ_ERROR;
    }

    queue->values[index] = value;
    wfring_enqueue(
        queue->available,
        queue->order,
        index,
        false,
        &queue->available_states[thread_id]);
    atomic_fetch_add_explicit(&queue->item_count, 1, memory_order_release);
    return MOSTREAM_WCQ_SUCCESS;
}

int mostream_wcq_u64_try_dequeue(
    mostream_wcq_u64 *queue,
    size_t thread_id,
    uint64_t *out_value)
{
    size_t item_count;
    size_t index;

    if (!mostream_valid_thread(queue, thread_id) || out_value == NULL)
        return MOSTREAM_WCQ_ERROR;

    item_count = atomic_load_explicit(&queue->item_count, memory_order_acquire);
    if (item_count == 0)
        return MOSTREAM_WCQ_WOULD_BLOCK;
    /* One CAS only: contention is reported as a transient WOULD_BLOCK. */
    if (!atomic_compare_exchange_strong_explicit(
            &queue->item_count,
            &item_count,
            item_count - 1,
            memory_order_acq_rel,
            memory_order_acquire))
        return MOSTREAM_WCQ_WOULD_BLOCK;

    index = wfring_dequeue(
        queue->available,
        queue->order,
        true,
        &queue->available_states[thread_id]);
    if (index == WFRING_EMPTY || index >= queue->capacity) {
        atomic_fetch_add_explicit(
            &queue->item_count, 1, memory_order_release);
        return MOSTREAM_WCQ_ERROR;
    }

    *out_value = queue->values[index];
    wfring_enqueue(
        queue->free_indices,
        queue->order,
        index,
        true,
        &queue->free_states[thread_id]);
    atomic_fetch_add_explicit(&queue->free_count, 1, memory_order_release);
    return MOSTREAM_WCQ_SUCCESS;
}

static void mostream_spin_pause(size_t attempts)
{
#if defined(__GNUC__) || defined(__clang__)
    __asm__ __volatile__("pause" ::: "memory");
#endif
    if ((attempts & 4095U) == 0)
        sched_yield();
}

int mostream_wcq_u64_enqueue(
    mostream_wcq_u64 *queue,
    size_t thread_id,
    uint64_t value)
{
    size_t attempts = 0;
    int result;

    do {
        result = mostream_wcq_u64_try_enqueue(queue, thread_id, value);
        if (result != MOSTREAM_WCQ_WOULD_BLOCK)
            return result;
        mostream_spin_pause(++attempts);
    } while (true);
}

int mostream_wcq_u64_dequeue(
    mostream_wcq_u64 *queue,
    size_t thread_id,
    uint64_t *out_value)
{
    size_t attempts = 0;
    int result;

    do {
        result = mostream_wcq_u64_try_dequeue(queue, thread_id, out_value);
        if (result != MOSTREAM_WCQ_WOULD_BLOCK)
            return result;
        mostream_spin_pause(++attempts);
    } while (true);
}
