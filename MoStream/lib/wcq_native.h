/*
 * Bounded uint64_t adapter for Ruslan Nikolaev's wCQ.
 *
 * The queue owns a fixed payload array and two bounded wCQ rings: one ring
 * contains published payload indices, the other contains free indices.
 * Values are therefore not restricted by wCQ's internal index encoding.
 */

#ifndef MOSTREAM_WCQ_NATIVE_H
#define MOSTREAM_WCQ_NATIVE_H 1

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mostream_wcq_u64 mostream_wcq_u64;

enum {
    MOSTREAM_WCQ_ERROR = -1,
    MOSTREAM_WCQ_WOULD_BLOCK = 0,
    MOSTREAM_WCQ_SUCCESS = 1
};

/*
 * Creates a queue with exactly `capacity` payload positions.
 *
 * Requirements:
 *   - capacity is a power of two and at least 8;
 *   - 1 <= max_threads <= capacity;
 *   - each concurrent caller uses a distinct thread_id in
 *     [0, max_threads). A thread_id may be reused after its previous call
 *     has returned.
 *
 * Returns 0 on success, otherwise an errno value (EINVAL, ENOMEM, ...).
 */
int mostream_wcq_u64_create(
    size_t capacity,
    size_t max_threads,
    mostream_wcq_u64 **out_queue);

/* Call only after all worker threads have stopped using the queue. */
void mostream_wcq_u64_destroy(mostream_wcq_u64 *queue);

size_t mostream_wcq_u64_capacity(const mostream_wcq_u64 *queue);
size_t mostream_wcq_u64_max_threads(const mostream_wcq_u64 *queue);

/*
 * Bounded attempts built from the upstream wCQ rings and one strong admission
 * CAS. The adapter has not been given a separate formal wait-freedom proof.
 * SUCCESS means the value was transferred. WOULD_BLOCK means no published
 * value/free position was observed, or that a concurrent caller won the
 * bounded admission CAS; it can therefore be a transient result. ERROR means
 * an invalid argument or a violated internal invariant. A thread paused after
 * admission temporarily owns that token, so it can also make another caller
 * observe WOULD_BLOCK until it resumes.
 */
int mostream_wcq_u64_try_enqueue(
    mostream_wcq_u64 *queue,
    size_t thread_id,
    uint64_t value);

int mostream_wcq_u64_try_dequeue(
    mostream_wcq_u64 *queue,
    size_t thread_id,
    uint64_t *out_value);

/*
 * Busy-waiting convenience operations. They are not wait-free: a full/empty
 * queue, repeated admission contention, or a paused token owner can delay
 * them indefinitely.
 */
int mostream_wcq_u64_enqueue(
    mostream_wcq_u64 *queue,
    size_t thread_id,
    uint64_t value);

int mostream_wcq_u64_dequeue(
    mostream_wcq_u64 *queue,
    size_t thread_id,
    uint64_t *out_value);

#ifdef __cplusplus
}
#endif

#endif /* MOSTREAM_WCQ_NATIVE_H */
