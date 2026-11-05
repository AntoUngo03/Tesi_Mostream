/* Test scheduler only. Every queue operation runs in the Mojo callback. */
#define _GNU_SOURCE
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

extern void mostream_wcq_test_worker(void *, uint64_t, uint64_t, uint64_t,
                                     uint64_t, uint64_t);
extern void mostream_wcq_test_dump(void *);
struct worker_args {
    void *queue;
    uint64_t control, seen, tid, count, consumers;
};
static void *worker(void *opaque) {
    struct worker_args *a = opaque;
    mostream_wcq_test_worker(a->queue, a->control, a->seen, a->tid,
                              a->count, a->consumers);
    return NULL;
}
int32_t mostream_wcq_test_run(void *queue, uint64_t control, uint64_t seen,
                            uint64_t count, uint64_t consumers) {
    if (consumers != 1 && consumers != 4) return -1;
    pthread_t threads[8];
    struct worker_args args[8];
    unsigned created = 0, workers = 4 + consumers;
    int error = 0;
    for (; created < workers; ++created) {
        args[created] = (struct worker_args){queue, control, seen, created, count, consumers};
        error = pthread_create(&threads[created], NULL, worker, &args[created]);
        if (error) break;
    }
    __atomic_store_n((uint64_t *)(uintptr_t)(control + 8), error ? 2 : 1, __ATOMIC_SEQ_CST);
    for (unsigned i = 0; i < created; ++i) {
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += 20;
        int result = pthread_timedjoin_np(threads[i], NULL, &deadline);
        if (result) {
            fprintf(stderr, "wCQ worker did not terminate: %d; progress:", result);
            for (unsigned j = 0; j < workers; ++j)
                fprintf(stderr, " %lu", (unsigned long)__atomic_load_n(
                    (uint64_t *)(uintptr_t)(control + 24 + j * 8), __ATOMIC_SEQ_CST));
            fputc('\n', stderr);
            mostream_wcq_test_dump(queue);
            fflush(NULL);
            _Exit(124);
        }
        if (result && !error) error = result;
    }
    return error;
}
