#define _GNU_SOURCE

#include "../MoStream/lib/wcq_native.h"

#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

struct stress_case {
    mostream_wcq_u64 *queue;
    pthread_barrier_t start;
    _Atomic unsigned char *seen;
    size_t *last_sequence;
    _Atomic size_t produced;
    _Atomic size_t consumed;
    _Atomic size_t producers_done;
    _Atomic int failed;
    _Atomic int finished;
    size_t producers;
    size_t consumers;
    size_t items_per_producer;
    size_t capacity;
};

struct worker {
    struct stress_case *test;
    size_t worker_id;
    size_t local_id;
};

struct bounded_case {
    mostream_wcq_u64 *queue;
    pthread_barrier_t start;
    uint64_t *accepted;
    _Atomic uint64_t next_value;
    _Atomic size_t successes;
    _Atomic size_t attempts;
    _Atomic int failed;
    _Atomic int finished;
    size_t producers;
    size_t capacity;
};

struct bounded_worker {
    struct bounded_case *test;
    size_t worker_id;
};

static void *producer_main(void *opaque)
{
    struct worker *worker = opaque;
    struct stress_case *test = worker->test;
    size_t i;

    pthread_barrier_wait(&test->start);
    for (i = 0; i < test->items_per_producer; ++i) {
        uint64_t value = worker->local_id * test->items_per_producer + i;
        if (mostream_wcq_u64_enqueue(test->queue, worker->worker_id, value) !=
            MOSTREAM_WCQ_SUCCESS) {
            atomic_store_explicit(&test->failed, 1, memory_order_relaxed);
            break;
        }
        atomic_fetch_add_explicit(&test->produced, 1, memory_order_relaxed);
    }
    atomic_fetch_add_explicit(&test->producers_done, 1, memory_order_relaxed);
    return NULL;
}

static void *consumer_main(void *opaque)
{
    struct worker *worker = opaque;
    struct stress_case *test = worker->test;
    size_t total = test->producers * test->items_per_producer;

    pthread_barrier_wait(&test->start);
    while (atomic_load_explicit(&test->consumed, memory_order_relaxed) < total &&
           atomic_load_explicit(&test->failed, memory_order_relaxed) == 0) {
        uint64_t value;
        int result = mostream_wcq_u64_try_dequeue(
            test->queue, worker->worker_id, &value);
        if (result == MOSTREAM_WCQ_WOULD_BLOCK)
            continue;
        if (result != MOSTREAM_WCQ_SUCCESS) {
            atomic_store_explicit(&test->failed, 1, memory_order_relaxed);
            break;
        }
        if (value >= total) {
            atomic_store_explicit(&test->failed, 1, memory_order_relaxed);
            break;
        }
        if (atomic_fetch_add_explicit(
                &test->seen[value], 1, memory_order_relaxed) != 0) {
            atomic_store_explicit(&test->failed, 1, memory_order_relaxed);
            break;
        }
        {
            size_t producer = value / test->items_per_producer;
            size_t sequence = value % test->items_per_producer;
            size_t *last = &test->last_sequence[
                worker->local_id * test->producers + producer];
            if (*last != SIZE_MAX && sequence <= *last) {
                atomic_store_explicit(&test->failed, 1, memory_order_relaxed);
                break;
            }
            *last = sequence;
        }
        atomic_fetch_add_explicit(&test->consumed, 1, memory_order_relaxed);
    }
    return NULL;
}

static void *watchdog_main(void *opaque)
{
    struct stress_case *test = opaque;
    const struct timespec interval = {.tv_sec = 0, .tv_nsec = 10000000};
    size_t tick;

    for (tick = 0; tick < 1000; ++tick) {
        if (atomic_load_explicit(&test->finished, memory_order_relaxed) != 0)
            return NULL;
        nanosleep(&interval, NULL);
    }

    fprintf(
        stderr,
        "TIMEOUT %zuP/%zuC capacity=%zu: produced=%zu consumed=%zu "
        "producers_done=%zu failed=%d\n",
        test->producers,
        test->consumers,
        test->capacity,
        atomic_load_explicit(&test->produced, memory_order_relaxed),
        atomic_load_explicit(&test->consumed, memory_order_relaxed),
        atomic_load_explicit(&test->producers_done, memory_order_relaxed),
        atomic_load_explicit(&test->failed, memory_order_relaxed));
    fflush(stderr);
    _Exit(EXIT_FAILURE);
}

static int api_contract_test(void)
{
    mostream_wcq_u64 *queue = NULL;
    uint64_t value = 0;
    int result = 1;

    if (mostream_wcq_u64_create(8, 1, NULL) == 0)
        return 1;
    if (mostream_wcq_u64_create(7, 1, &queue) == 0 || queue != NULL)
        return 1;
    if (mostream_wcq_u64_create(8, 0, &queue) == 0 || queue != NULL)
        return 1;
    if (mostream_wcq_u64_create(8, 9, &queue) == 0 || queue != NULL)
        return 1;
    if (mostream_wcq_u64_create(8, 1, &queue) != 0)
        return 1;
    if (mostream_wcq_u64_capacity(queue) != 8 ||
        mostream_wcq_u64_max_threads(queue) != 1)
        goto cleanup;
    if (mostream_wcq_u64_try_enqueue(queue, 1, 42) !=
            MOSTREAM_WCQ_ERROR ||
        mostream_wcq_u64_try_dequeue(queue, 1, &value) !=
            MOSTREAM_WCQ_ERROR ||
        mostream_wcq_u64_try_dequeue(queue, 0, NULL) !=
            MOSTREAM_WCQ_ERROR)
        goto cleanup;

    result = 0;

cleanup:
    mostream_wcq_u64_destroy(queue);
    if (result == 0)
        puts("PASS native API argument/metadata contract");
    return result;
}

static int sequential_wrap_test(size_t capacity, size_t cycles)
{
    mostream_wcq_u64 *queue = NULL;
    size_t cycle;
    size_t i;
    uint64_t value = 0;
    int result = 1;

    if (mostream_wcq_u64_create(capacity, 1, &queue) != 0)
        return 1;
    if (mostream_wcq_u64_capacity(queue) != capacity ||
        mostream_wcq_u64_max_threads(queue) != 1)
        goto cleanup;
    value = UINT64_C(0x0123456789abcdef);
    if (mostream_wcq_u64_try_dequeue(queue, 0, &value) !=
            MOSTREAM_WCQ_WOULD_BLOCK ||
        value != UINT64_C(0x0123456789abcdef))
        goto cleanup;

    for (cycle = 0; cycle < cycles; ++cycle) {
        for (i = 0; i < capacity; ++i) {
            uint64_t expected =
                cycle == 0 && i == capacity - 1
                    ? UINT64_MAX
                    : (uint64_t)(cycle * capacity + i);
            if (mostream_wcq_u64_try_enqueue(queue, 0, expected) !=
                MOSTREAM_WCQ_SUCCESS)
                goto cleanup;
        }
        if (mostream_wcq_u64_try_enqueue(queue, 0, UINT64_C(42)) !=
            MOSTREAM_WCQ_WOULD_BLOCK)
            goto cleanup;

        for (i = 0; i < capacity; ++i) {
            uint64_t expected =
                cycle == 0 && i == capacity - 1
                    ? UINT64_MAX
                    : (uint64_t)(cycle * capacity + i);
            if (mostream_wcq_u64_try_dequeue(queue, 0, &value) !=
                    MOSTREAM_WCQ_SUCCESS ||
                value != expected)
                goto cleanup;
        }
        value = UINT64_C(0xfedcba9876543210);
        if (mostream_wcq_u64_try_dequeue(queue, 0, &value) !=
                MOSTREAM_WCQ_WOULD_BLOCK ||
            value != UINT64_C(0xfedcba9876543210))
            goto cleanup;
    }

    result = 0;

cleanup:
    mostream_wcq_u64_destroy(queue);
    if (result == 0) {
        printf(
            "PASS sequential capacity=%zu cycles=%zu bounded/FIFO/wrap/uint64\n",
            capacity, cycles);
    }
    return result;
}

static int concurrent_exact_once_test(
    size_t producers,
    size_t consumers,
    size_t items_per_producer,
    size_t capacity)
{
    struct stress_case test = {0};
    size_t threads_count = producers + consumers;
    size_t total = producers * items_per_producer;
    pthread_t *threads = calloc(threads_count, sizeof(*threads));
    pthread_t watchdog;
    struct worker *workers = calloc(threads_count, sizeof(*workers));
    size_t i;
    int result = 1;

    if (producers == 0 || consumers == 0 || items_per_producer == 0 ||
        threads_count > capacity ||
        producers > SIZE_MAX / items_per_producer ||
        consumers > SIZE_MAX / producers)
        return 1;

    test.seen = calloc(total, sizeof(*test.seen));
    test.last_sequence = malloc(
        consumers * producers * sizeof(*test.last_sequence));
    test.producers = producers;
    test.consumers = consumers;
    test.items_per_producer = items_per_producer;
    test.capacity = capacity;
    if (threads == NULL || workers == NULL || test.seen == NULL ||
        test.last_sequence == NULL)
        goto cleanup;
    for (i = 0; i < consumers * producers; ++i)
        test.last_sequence[i] = SIZE_MAX;
    if (mostream_wcq_u64_create(capacity, threads_count, &test.queue) != 0)
        goto cleanup;
    if (pthread_barrier_init(&test.start, NULL, (unsigned)threads_count) != 0)
        goto cleanup;

    for (i = 0; i < threads_count; ++i) {
        workers[i].test = &test;
        workers[i].worker_id = i;
        if (i < producers) {
            workers[i].local_id = i;
            if (pthread_create(&threads[i], NULL, producer_main, &workers[i]) != 0)
                abort();
        } else {
            workers[i].local_id = i - producers;
            if (pthread_create(&threads[i], NULL, consumer_main, &workers[i]) != 0)
                abort();
        }
    }
    if (pthread_create(&watchdog, NULL, watchdog_main, &test) != 0)
        abort();
    for (i = 0; i < threads_count; ++i)
        pthread_join(threads[i], NULL);
    atomic_store_explicit(&test.finished, 1, memory_order_relaxed);
    pthread_join(watchdog, NULL);

    if (atomic_load_explicit(&test.failed, memory_order_relaxed) != 0 ||
        atomic_load_explicit(&test.produced, memory_order_relaxed) != total ||
        atomic_load_explicit(&test.consumed, memory_order_relaxed) != total ||
        atomic_load_explicit(
            &test.producers_done, memory_order_relaxed) != producers)
        goto destroy_barrier;
    for (i = 0; i < total; ++i) {
        if (atomic_load_explicit(&test.seen[i], memory_order_relaxed) != 1)
            goto destroy_barrier;
    }
    if (consumers == 1) {
        for (i = 0; i < producers; ++i) {
            if (test.last_sequence[i] != items_per_producer - 1)
                goto destroy_barrier;
        }
    }
    {
        uint64_t leftover;
        if (mostream_wcq_u64_try_dequeue(test.queue, 0, &leftover) !=
            MOSTREAM_WCQ_WOULD_BLOCK)
            goto destroy_barrier;
    }

    printf(
        "PASS %zuP/%zuC capacity=%zu items=%zu exact-once/%sFIFO\n",
        producers, consumers, capacity, total,
        consumers == 1 ? "per-producer-" : "per-consumer-");
    result = 0;

destroy_barrier:
    pthread_barrier_destroy(&test.start);
cleanup:
    mostream_wcq_u64_destroy(test.queue);
    free((void *)test.seen);
    free(test.last_sequence);
    free(workers);
    free(threads);
    return result;
}

static void *bounded_producer_main(void *opaque)
{
    struct bounded_worker *worker = opaque;
    struct bounded_case *test = worker->test;
    size_t local_attempts = 0;

    pthread_barrier_wait(&test->start);
    while (atomic_load_explicit(&test->failed, memory_order_relaxed) == 0) {
        uint64_t value;
        size_t rank;
        int result;

        if (atomic_load_explicit(
                &test->successes, memory_order_relaxed) >= test->capacity)
            break;

        value = atomic_fetch_add_explicit(
            &test->next_value, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&test->attempts, 1, memory_order_relaxed);
        result = mostream_wcq_u64_try_enqueue(
            test->queue, worker->worker_id, value);
        if (result == MOSTREAM_WCQ_WOULD_BLOCK) {
            if ((++local_attempts & 4095U) == 0)
                sched_yield();
            continue;
        }
        if (result != MOSTREAM_WCQ_SUCCESS) {
            atomic_store_explicit(&test->failed, 1, memory_order_relaxed);
            break;
        }

        rank = atomic_fetch_add_explicit(
            &test->successes, 1, memory_order_relaxed);
        if (rank >= test->capacity) {
            atomic_store_explicit(&test->failed, 1, memory_order_relaxed);
            break;
        }
        test->accepted[rank] = value;
    }
    return NULL;
}

static void *bounded_watchdog_main(void *opaque)
{
    struct bounded_case *test = opaque;
    const struct timespec interval = {.tv_sec = 0, .tv_nsec = 10000000};
    size_t tick;

    for (tick = 0; tick < 1000; ++tick) {
        if (atomic_load_explicit(&test->finished, memory_order_relaxed) != 0)
            return NULL;
        nanosleep(&interval, NULL);
    }

    fprintf(
        stderr,
        "TIMEOUT bounded-fill %zuP capacity=%zu: successes=%zu "
        "attempts=%zu failed=%d\n",
        test->producers,
        test->capacity,
        atomic_load_explicit(&test->successes, memory_order_relaxed),
        atomic_load_explicit(&test->attempts, memory_order_relaxed),
        atomic_load_explicit(&test->failed, memory_order_relaxed));
    fflush(stderr);
    _Exit(EXIT_FAILURE);
}

static int compare_u64(const void *left_pointer, const void *right_pointer)
{
    uint64_t left = *(const uint64_t *)left_pointer;
    uint64_t right = *(const uint64_t *)right_pointer;
    return left < right ? -1 : left > right;
}

static int concurrent_bounded_fill_test(size_t producers, size_t capacity)
{
    struct bounded_case test = {0};
    pthread_t *threads = calloc(producers, sizeof(*threads));
    pthread_t watchdog;
    struct bounded_worker *workers = calloc(producers, sizeof(*workers));
    uint64_t *drained = calloc(capacity, sizeof(*drained));
    bool barrier_initialized = false;
    size_t i;
    int result = 1;

    if (producers == 0 || producers > capacity)
        goto cleanup;
    test.accepted = calloc(capacity, sizeof(*test.accepted));
    test.producers = producers;
    test.capacity = capacity;
    atomic_init(&test.next_value, 1);
    if (threads == NULL || workers == NULL || drained == NULL ||
        test.accepted == NULL)
        goto cleanup;
    if (mostream_wcq_u64_create(capacity, producers, &test.queue) != 0)
        goto cleanup;
    if (pthread_barrier_init(&test.start, NULL, (unsigned)producers) != 0)
        goto cleanup;
    barrier_initialized = true;

    for (i = 0; i < producers; ++i) {
        workers[i].test = &test;
        workers[i].worker_id = i;
        if (pthread_create(
                &threads[i], NULL, bounded_producer_main, &workers[i]) != 0)
            abort();
    }
    if (pthread_create(&watchdog, NULL, bounded_watchdog_main, &test) != 0)
        abort();
    for (i = 0; i < producers; ++i)
        pthread_join(threads[i], NULL);
    atomic_store_explicit(&test.finished, 1, memory_order_relaxed);
    pthread_join(watchdog, NULL);

    if (atomic_load_explicit(&test.failed, memory_order_relaxed) != 0 ||
        atomic_load_explicit(&test.successes, memory_order_relaxed) != capacity)
        goto cleanup;
    if (mostream_wcq_u64_try_enqueue(test.queue, 0, UINT64_MAX) !=
        MOSTREAM_WCQ_WOULD_BLOCK)
        goto cleanup;

    for (i = 0; i < capacity; ++i) {
        if (mostream_wcq_u64_try_dequeue(test.queue, 0, &drained[i]) !=
            MOSTREAM_WCQ_SUCCESS)
            goto cleanup;
    }
    {
        uint64_t unchanged = UINT64_C(0xcafebabedeadbeef);
        if (mostream_wcq_u64_try_dequeue(test.queue, 0, &unchanged) !=
                MOSTREAM_WCQ_WOULD_BLOCK ||
            unchanged != UINT64_C(0xcafebabedeadbeef))
            goto cleanup;
    }

    qsort(test.accepted, capacity, sizeof(*test.accepted), compare_u64);
    qsort(drained, capacity, sizeof(*drained), compare_u64);
    for (i = 0; i < capacity; ++i) {
        if (test.accepted[i] != drained[i])
            goto cleanup;
    }

    printf(
        "PASS bounded-fill %zuP capacity=%zu successes=%zu attempts=%zu\n",
        producers,
        capacity,
        atomic_load_explicit(&test.successes, memory_order_relaxed),
        atomic_load_explicit(&test.attempts, memory_order_relaxed));
    result = 0;

cleanup:
    if (result != 0 && test.queue != NULL) {
        fprintf(
            stderr,
            "FAIL bounded-fill %zuP capacity=%zu successes=%zu attempts=%zu "
            "failed=%d\n",
            producers,
            capacity,
            atomic_load_explicit(&test.successes, memory_order_relaxed),
            atomic_load_explicit(&test.attempts, memory_order_relaxed),
            atomic_load_explicit(&test.failed, memory_order_relaxed));
    }
    if (barrier_initialized)
        pthread_barrier_destroy(&test.start);
    mostream_wcq_u64_destroy(test.queue);
    free(test.accepted);
    free(drained);
    free(workers);
    free(threads);
    return result;
}

static int run_exact_matrix(
    size_t capacity, size_t items_per_producer, size_t repeats)
{
    static const size_t concurrency[] = {1, 2, 4, 8};
    size_t repeat;
    size_t i;

    for (repeat = 0; repeat < repeats; ++repeat) {
        for (i = 0; i < sizeof(concurrency) / sizeof(concurrency[0]); ++i) {
            size_t workers = concurrency[i];
            if (concurrent_exact_once_test(
                    workers,
                    workers,
                    items_per_producer,
                    capacity) != 0) {
                fprintf(
                    stderr,
                    "FAIL repeat=%zu case=%zuP/%zuC capacity=%zu\n",
                    repeat, workers, workers, capacity);
                return 1;
            }
        }
    }
    return 0;
}

int main(void)
{
    static const size_t concurrency[] = {1, 2, 4, 8};
    size_t i;

    if (api_contract_test() != 0) {
        fputs("FAIL native API contract test\n", stderr);
        return EXIT_FAILURE;
    }
    if (sequential_wrap_test(8, 25000) != 0 ||
        sequential_wrap_test(1024, 128) != 0) {
        fputs("FAIL sequential boundary/wrap test\n", stderr);
        return EXIT_FAILURE;
    }

    for (i = 0; i < sizeof(concurrency) / sizeof(concurrency[0]); ++i) {
        if (concurrent_bounded_fill_test(concurrency[i], 16) != 0)
            return EXIT_FAILURE;
    }
    if (concurrent_bounded_fill_test(8, 1024) != 0)
        return EXIT_FAILURE;

    /* Tiny rings maximize full/empty transitions; the middle size is the
       practical benchmark default; the large ring catches mapping/allocation
       errors without making the stress suite needlessly long. */
    if (run_exact_matrix(16, 20000, 3) != 0 ||
        run_exact_matrix(1024, 100000, 5) != 0 ||
        run_exact_matrix(65536, 10000, 1) != 0)
        return EXIT_FAILURE;

    if (concurrent_exact_once_test(8, 1, 25000, 16) != 0) {
        fputs("FAIL 8P/1C per-producer FIFO case\n", stderr);
        return EXIT_FAILURE;
    }
    if (concurrent_exact_once_test(1, 8, 25000, 16) != 0) {
        fputs("FAIL 1P/8C exact-once/FIFO case\n", stderr);
        return EXIT_FAILURE;
    }

    puts("PASS all native wCQ stress tests");
    return EXIT_SUCCESS;
}
