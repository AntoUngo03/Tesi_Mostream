/*
 * Padico -- a High Performance Parallel and Distributed Computing Environment
 * Copyright (c) 2002-2026 INRIA and the University of Rennes 1
 * Alexandre DENIS <Alexandre.Denis@inria.fr>
 * Christian PEREZ <Christian.Perez@inria.fr>
 *
 * The software has been registered at the Agency for the Protection of
 * Programs (APP) under the number IDDN.FR.001.260013.000.S.P.2002.000.10000.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */
 
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#ifdef PUK_LFQUEUE_NBLFQ2
#include <stdatomic.h>
#endif /* PUK_LFQUEUE_NBLFQ2 */
 
#include "Puk-tagged-ptr.h"
#include "Puk-config.h"
 
 
/* index in the array are in the range 0..NBLFQ_SIZE-1
 * tags (seq number) are the number of loops, in the range 0..PUK_NBLFQ_SEQ_WRAP-1
 */
 
#define PUK_NBLFQ_SEQ_WRAP (PUK_TAGGED_PTR_TAG_MAX + 1)
 
#define PUK_NBLFQ_ALIGN 64
 
#if defined(PUK_ENABLE_PROFILE) && !defined(__cplusplus) && defined(PUK_PROFILE_H)
/* do not profile nblfq in C++ since all nblfq is inlined
 * but Puk profiling does not compile in C++ */
#define NBLFQ_PROFILE 1
#endif
 
 
#ifdef NBLFQ_PROFILE
struct nblfq_profile_s
{
  unsigned long long n_enqueue;
  unsigned long long n_dequeue;
  unsigned long long n_enqueue_full;
  unsigned long long n_dequeue_empty;
  unsigned long long n_enqueue_inconsistency;
  unsigned long long n_dequeue_inconsistency2;
  unsigned long long n_dequeue_inconsistency3;
  unsigned long long n_enqueue_race;
  unsigned long long n_dequeue_race;
};
static inline void nblfq_profile_init(struct nblfq_profile_s*p, const char*ename)
{
  puk_profile_var_defx(unsigned_long_long, counter, &p->n_enqueue, 0,
                       "total number of enqueue operation",
                       "puk_nblfq.%s.%p.n_enqueue", ename, p);
  puk_profile_var_defx(unsigned_long_long, counter, &p->n_dequeue, 0,
                       "total number of dequeue operation",
                       "puk_nblfq.%s.%p.n_dequeue", ename, p);
  puk_profile_var_defx(unsigned_long_long, counter, &p->n_enqueue_full, 0,
                       "total number of enqueue attempted while queue was full",
                       "puk_nblfq.%s.%p.n_enqueue_full", ename, p);
  puk_profile_var_defx(unsigned_long_long, counter, &p->n_dequeue_empty, 0,
                       "total number of dequeue attempted while queue was empty",
                       "puk_nblfq.%s.%p.n_dequeue_empty", ename, p);
  puk_profile_var_defx(unsigned_long_long, counter, &p->n_enqueue_inconsistency, 0,
                       "total number of inconsistent state detected while enqueueing",
                       "puk_nblfq.%s.%p.n_enqueue_inconsistency", ename, p);
  puk_profile_var_defx(unsigned_long_long, counter, &p->n_dequeue_inconsistency2, 0,
                      "total number of inconsistent state detected while dequeueing",
                       "puk_nblfq.%s.%p.n_dequeue_inconsistency2", ename, p);
  puk_profile_var_defx(unsigned_long_long, counter, &p->n_dequeue_inconsistency3, 0,
                       "total number of inconsistent state detected while dequeueing",
                       "puk_nblfq.%s.%p.n_dequeue_inconsistency3", ename, p);
  puk_profile_var_defx(unsigned_long_long, counter, &p->n_enqueue_race, 0,
                       "total number of race detected by CAS while enqueueing",
                       "puk_nblfq.%s.%p.n_enqueue_race", ename, p);
  puk_profile_var_defx(unsigned_long_long, counter, &p->n_dequeue_race, 0,
                       "total number of race detected by CAS while dequeueing",
                       "puk_nblfq.%s.%p.n_dequeue_race", ename, p);
}
#define nblfq_profile_inc(I) __sync_fetch_and_add(&I, 1);
#else
struct nblfq_profile_s
{ /* empty */ };
static inline void nblfq_profile_init(struct nblfq_profile_s*p  __attribute__((unused)), const char*ename __attribute__((unused)))
{ };
#define nblfq_profile_inc(I)
#endif
 
#define PUK_LFQUEUE_NBLFQ_TYPE(ENAME, TYPE, LFQUEUE_NULL, NBLFQ_SIZE)   \
  typedef TYPE ENAME ## _lfqueue_elem_t;                                \
                                                                        \
  struct ENAME ## _lfqueue_s                                            \
  {                                                                     \
    __attribute__((aligned(PUK_NBLFQ_ALIGN))) puk_tagged_ptr_t _array[NBLFQ_SIZE];  \
    __attribute__((aligned(PUK_NBLFQ_ALIGN))) volatile unsigned _head;  \
    __attribute__((aligned(PUK_NBLFQ_ALIGN))) volatile unsigned _tail;  \
    struct nblfq_profile_s _profile;                                    \
  };                                                                    \
  /* helper functions */                                                \
                                    \
  static inline int ENAME ## _nblfq_isempty(puk_tagged_ptr_t u)         \
  {                                                                     \
    return (puk_tagged_ptr_get_ptr(u) == NULL);                         \
  }                                                                     \
                               \
  __attribute__((unused))                                               \
  static inline uintptr_t ENAME ## _nblfq_getseq(puk_tagged_ptr_t u)    \
  {                                                                     \
    return puk_tagged_ptr_get_tag(u);                                   \
  }                                                                     \
  static inline uintptr_t ENAME ## _nblfq_nextseq(uintptr_t s)          \
  {                                                                     \
    return (s + 1) % PUK_NBLFQ_SEQ_WRAP;                                \
  }                                                                     \
  static inline uintptr_t ENAME ## _nblfq_prevseq(uintptr_t s)          \
  {                                                                     \
    return (s + PUK_NBLFQ_SEQ_WRAP - 1) % PUK_NBLFQ_SEQ_WRAP;           \
  }                                                                     \
  static inline unsigned ENAME ## _nblfq_remap(unsigned i)              \
  {                                                                     \
    /* allow index remapping, but do not enable by default */           \
    /* const unsigned stride = 32;                                       \
       const unsigned x = i % stride;                                   \
       const unsigned y = i / stride;                                   \
       return y + x * (NBLFQ_SIZE / stride); */                         \
    return i;                                                           \
  }                                                                     \
  static inline puk_tagged_ptr_t ENAME ## _nblfq_element(struct ENAME ## _lfqueue_s*q, unsigned i) \
  {                                                                     \
    return q->_array[ENAME ## _nblfq_remap(i)];                         \
  }                                                                     \
  static inline puk_tagged_ptr_t*ENAME ## _nblfq_pelement(struct ENAME ## _lfqueue_s*q, unsigned i) \
  {                                                                     \
    return &q->_array[ENAME ## _nblfq_remap(i)];                        \
  }                                                                     \
 \
  static inline int ENAME ## _nblfq_compare(unsigned i1, puk_tagged_ptr_t u1, unsigned i2, puk_tagged_ptr_t u2) \
  {                                                                     \
    uintptr_t s1 = puk_tagged_ptr_get_tag(u1);                          \
    uintptr_t s2 = puk_tagged_ptr_get_tag(u2);                          \
    assert(!((s1 == s2) && (i1 == i2))); /* seqs are not supposed to be equal */ \
    if(s1 == s2)                                                        \
      {                                                                 \
        return (i1 < i2);                                               \
      }                                                                 \
    else if((s2 + PUK_NBLFQ_SEQ_WRAP - s1) % PUK_NBLFQ_SEQ_WRAP < PUK_NBLFQ_SEQ_WRAP / 4) \
      {                                                                 \
        return 1;                                                       \
      }                                                                 \
    else if((s1 + PUK_NBLFQ_SEQ_WRAP - s2) % PUK_NBLFQ_SEQ_WRAP < PUK_NBLFQ_SEQ_WRAP / 4) \
      {                                                                 \
        return 0;                                                       \
      }                                                                 \
    else                                                                \
      {                                                                 \
        fprintf(stderr, "# nblfq: undecided comparison between seqs.\n"); \
        abort();                                                        \
      }                                                                 \
  }                                                                     \
                                                                        \
                                     \
  static inline unsigned ENAME ## _nblfq_chase_head(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
    unsigned head = q->_head;                                           \
    unsigned prev = (head + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    puk_tagged_ptr_t p = ENAME ## _nblfq_element(q, prev);              \
    puk_tagged_ptr_t u = ENAME ## _nblfq_element(q, head);              \
    for(;;)                                                             \
      {                                                                 \
        /* find an empty cell with a predecessor either non-empty or with a higher seq) */ \
        if((!ENAME ## _nblfq_isempty(p)) && (ENAME ## _nblfq_isempty(u))) \
          return head; /* empty cell with non-empty predecessor: regular head */ \
        if(! ENAME ## _nblfq_compare(prev, p, head, u))                 \
          { /* out-of-order -> wrap point */                            \
            if(ENAME ## _nblfq_isempty(p) && ENAME ## _nblfq_isempty(u)) \
              return head; /* empty list */                             \
            if((!ENAME ## _nblfq_isempty(p)) && (!ENAME ## _nblfq_isempty(u))) \
              { /* list full */                                         \
                q->_head = head;                                        \
                return head;                                            \
              }                                                         \
          }                                                             \
        prev = head;                                                    \
        head = (head + 1) % (NBLFQ_SIZE);                               \
        p = u;                                                          \
        u = ENAME ## _nblfq_element(q, head);                           \
      }                                                                 \
  }                                                                     \
                                     \
  static inline unsigned ENAME ## _nblfq_chase_tail(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
    unsigned tail = q->_tail;                                           \
    unsigned prev = (tail + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    puk_tagged_ptr_t p = ENAME ## _nblfq_element(q, prev);              \
    puk_tagged_ptr_t u = ENAME ## _nblfq_element(q, tail);              \
    while(ENAME ## _nblfq_compare(prev, p, tail, u))                    \
      {                                                                 \
        prev = tail;                                                    \
        tail = (tail + 1) % (NBLFQ_SIZE);                               \
        p = u;                                                          \
        u = ENAME ## _nblfq_element(q, tail);                           \
      }                                                                 \
    return tail;                                                        \
  }                                                                     \
  __attribute__((unused))                                               \
  static inline void ENAME ## _lfqueue_init(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
    assert(sizeof(TYPE) == sizeof(void*));                              \
    assert((LFQUEUE_NULL) == NULL);                                     \
    int i;                                                              \
    for(i = 0; i < (NBLFQ_SIZE); i++)                                   \
      {                                                                 \
        q->_array[i] = puk_tagged_ptr_build(LFQUEUE_NULL, 0);           \
      }                                                                 \
    q->_head = 0;                                                       \
    q->_tail = 0;                                                       \
    nblfq_profile_init(&q->_profile, #ENAME);                           \
  }                                                                     \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_enqueue_ext(struct ENAME ## _lfqueue_s*q, ENAME ## _lfqueue_elem_t e, int single) \
  {                                                                     \
    nblfq_profile_inc(q->_profile.n_enqueue);                           \
    int r = 0;                                                          \
  retry:                                                                \
    ;                                                                   \
    /* chase head */                                                    \
    unsigned head = q->_head;                                           \
    unsigned prev = (head + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    puk_tagged_ptr_t p = ENAME ## _nblfq_element(q, prev);              \
    puk_tagged_ptr_t u = ENAME ## _nblfq_element(q, head);              \
    for(;;)                                                             \
      {                                                                 \
        /* find an empty cell with a predecessor either non-empty or with a higher seq) */ \
        if((!ENAME ## _nblfq_isempty(p)) && (ENAME ## _nblfq_isempty(u))) \
          break; /* empty cell with non-empty predecessor: regular head */ \
        if(! ENAME ## _nblfq_compare(prev, p, head, u))                 \
          { /* out-of-order -> wrap point */                            \
            if(ENAME ## _nblfq_isempty(p) && ENAME ## _nblfq_isempty(u)) \
              { /* empty list */                                        \
                break;                                                  \
              }                                                         \
            if((!ENAME ## _nblfq_isempty(p)) && (!ENAME ## _nblfq_isempty(u))) \
              { /* list full */                                         \
                q->_head = head;                                        \
                nblfq_profile_inc(q->_profile.n_enqueue_full);          \
                return -1;                                              \
              }                                                         \
          }                                                             \
        prev = head;                                                    \
        head = (head + 1) % (NBLFQ_SIZE);                               \
        p = u;                                                          \
        u = ENAME ## _nblfq_element(q, head);                           \
      }                                                                 \
    /* compute seq number to use */                                     \
    const uintptr_t sp = puk_tagged_ptr_get_tag(p);                     \
    uintptr_t s = sp;                                                   \
    if(ENAME ## _nblfq_isempty(p))                                      \
      { /* list is empty => p = tail */                                 \
        s = ENAME ## _nblfq_prevseq(sp);                                \
      }                                                                 \
    if(head == 0)                                                       \
      { /* wraping around */                                            \
        s = ENAME ## _nblfq_nextseq(s);                                 \
      }                                                                 \
    /* try to insert value */                                           \
    const puk_tagged_ptr_t v = puk_tagged_ptr_build(e, s);              \
    const puk_tagged_ptr_t u0 = puk_tagged_ptr_build(NULL, s);          \
    if(__sync_bool_compare_and_swap(ENAME ## _nblfq_pelement(q, head), u0, v)) \
      {                                                                 \
        q->_head = (head + 1) % (NBLFQ_SIZE);                           \
      }                                                                 \
    else                                                                \
      {                                                                 \
        nblfq_profile_inc(q->_profile.n_enqueue_race);                  \
        puk_lfbackoff(r++);                                             \
        /* not needed since __sync_bool_compare_and_swap already performs a memory barrier */ \
        /* __sync_synchronize(); */                                     \
        goto retry;                                                     \
      }                                                                 \
    return 0;                                                           \
  }                                                                     \
                                                                        \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_enqueue(struct ENAME ## _lfqueue_s*queue, ENAME ## _lfqueue_elem_t e) \
  { return ENAME ## _lfqueue_enqueue_ext(queue, e, 0); }                \
                                                                        \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_enqueue_single_writer(struct ENAME ## _lfqueue_s*queue, ENAME ## _lfqueue_elem_t e) \
  { return ENAME ## _lfqueue_enqueue_ext(queue, e, 1); }                \
                                                                        \
  __attribute__((unused))                                               \
  static inline ENAME ## _lfqueue_elem_t ENAME ## _lfqueue_dequeue_ext(struct ENAME ## _lfqueue_s*q, int single) \
  {                                                                     \
    nblfq_profile_inc(q->_profile.n_dequeue);                           \
    int r = 0;                                                          \
  retry:                                                                \
    ;                                                                   \
    /* chase tail */                                                    \
    unsigned tail = q->_tail;                                           \
    unsigned prev = (tail + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    puk_tagged_ptr_t p = ENAME ## _nblfq_element(q, prev);              \
    puk_tagged_ptr_t u = ENAME ## _nblfq_element(q, tail);              \
    while(ENAME ## _nblfq_compare(prev, p, tail, u))                    \
      {                                                                 \
        /* find a cell whose predecessor has a higher seq */            \
        prev = tail;                                                    \
        tail = (tail + 1) % (NBLFQ_SIZE);                               \
        p = u;                                                          \
        u = ENAME ## _nblfq_element(q, tail);                           \
      }                                                                 \
    /* check whether queue is empty */                                  \
    if(ENAME ## _nblfq_isempty(u))                                      \
      {                                                                 \
        q->_tail = tail;                                                \
        nblfq_profile_inc(q->_profile.n_dequeue_empty);                 \
        return LFQUEUE_NULL; /* empty queue */                          \
      }                                                                 \
    /* compute seq number to use */                                     \
    const uintptr_t su = puk_tagged_ptr_get_tag(u);                     \
    const uintptr_t s = ENAME ## _nblfq_nextseq(su);                    \
    const puk_tagged_ptr_t v = puk_tagged_ptr_build(LFQUEUE_NULL, s);   \
    /* try to remove value */                                           \
    if(__sync_bool_compare_and_swap(ENAME ## _nblfq_pelement(q, tail), u, v)) \
      {                                                                 \
        q->_tail = (tail + 1) % (NBLFQ_SIZE);                           \
        assert(!ENAME ## _nblfq_isempty(u));                            \
        return puk_tagged_ptr_get_ptr(u);                               \
      }                                                                 \
    else                                                                \
      {                                                                 \
        nblfq_profile_inc(q->_profile.n_dequeue_race);                  \
        puk_lfbackoff(r++);                                             \
        /* not needed since __sync_bool_compare_and_swap already performs a memory barrier */ \
        /* __sync_synchronize(); */                                     \
        goto retry;                                                     \
      }                                                                 \
    return LFQUEUE_NULL;                                                \
  }                                                                     \
                                                                        \
  __attribute__((unused))                                               \
  static inline ENAME ## _lfqueue_elem_t ENAME ## _lfqueue_dequeue(struct ENAME ## _lfqueue_s*queue) \
  { return ENAME ## _lfqueue_dequeue_ext(queue, 0); }                   \
                                                                        \
  __attribute__((unused))                                               \
  static inline ENAME ## _lfqueue_elem_t ENAME ## _lfqueue_dequeue_single_reader(struct ENAME ## _lfqueue_s*queue) \
  { return ENAME ## _lfqueue_dequeue_ext(queue, 1); }                   \
                                                                        \
                       \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_empty(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
    unsigned tail = ENAME ## _nblfq_chase_tail(q);                      \
    /* TODO- do not recompute prev, p, u */                             \
    unsigned prev = (tail + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    puk_tagged_ptr_t p = ENAME ## _nblfq_element(q, prev);              \
    puk_tagged_ptr_t u = ENAME ## _nblfq_element(q, tail);              \
    if(ENAME ## _nblfq_isempty(p) && ENAME ## _nblfq_isempty(u))        \
      return 1;                                                         \
    else                                                                \
      return 0;                                                         \
  }                                                                     \
                                                                   \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_load(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
  retry:                                                                \
    ;                                                                   \
    unsigned head = q->_head;                                           \
    unsigned tail = q->_tail;                                           \
    if(head != tail)                                                    \
      {                                                                 \
        return (head + (NBLFQ_SIZE) - tail) % (NBLFQ_SIZE);             \
      }                                                                 \
    else                                                                \
      {                                                                 \
        unsigned prev = (tail + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);       \
        puk_tagged_ptr_t p = ENAME ## _nblfq_element(q, prev);          \
        puk_tagged_ptr_t u = ENAME ## _nblfq_element(q, tail);          \
        if(ENAME ## _nblfq_isempty(p) && ENAME ## _nblfq_isempty(u))    \
          return 0;                                                     \
        else if((!ENAME ## _nblfq_isempty(p)) && (!ENAME ## _nblfq_isempty(u))) \
          return (NBLFQ_SIZE);                                          \
        else                                                            \
          { /* inconsistent state; retry */                             \
            q->_head = ENAME ## _nblfq_chase_head(q);                   \
            q->_tail = ENAME ## _nblfq_chase_tail(q);                   \
            goto retry;                                                 \
          }                                                             \
      }                                                                 \
  }                                                                     \
                                   \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_size(struct ENAME ## _lfqueue_s*queue __attribute__((unused))) \
  {                                                                     \
    return (NBLFQ_SIZE);                                                \
  }
 
 
#if PUK_SIZEOF_VOIDP == 8
typedef uint64_t puk_nblfq2_seq_t;
typedef __int128 puk_nblfq2_pair_t;
#elif PUK_SIZEOF_VOIDP == 4
typedef uint32_t puk_nblfq2_seq_t;
typedef uint64_t puk_nblfq2_pair_t;
#undef PUK_LFQUEUE_NBLFQ_TYPE
/* plain NBLFQ not available in 32 bits since we don't have tagged pointers */
#define PUK_LFQUEUE_NBLFQ_TYPE PUK_LFQUEUE_NBLFQ2_TYPE
#elif defined(PUK_SIZEOF_VOIDP)
#warning "unsupported word size in nblfq2"
#else
#error "PUK_SIZEOF_VOIDP undefined in nblfq2"
#endif
 
#define PUK_LFQUEUE_NBLFQ2_TYPE(ENAME, TYPE, LFQUEUE_NULL, NBLFQ_SIZE)  \
  typedef TYPE ENAME ## _lfqueue_elem_t;                                \
  struct ENAME ## _nblfq2_cell_s                                        \
  {                                                                     \
    TYPE _value;                                                        \
    puk_nblfq2_seq_t _seq;                                              \
  };                                                                    \
  union ENAME ## _nblfq2_cell_u                                         \
  {                                                                     \
    struct ENAME ## _nblfq2_cell_s cell;                                \
    puk_nblfq2_pair_t i;                                                \
  };                                                                    \
  struct ENAME ## _lfqueue_s                                            \
  {                                                                     \
    union ENAME ## _nblfq2_cell_u _array[NBLFQ_SIZE];                   \
    __attribute__((aligned(PUK_NBLFQ_ALIGN))) volatile unsigned _head;  \
    __attribute__((aligned(PUK_NBLFQ_ALIGN))) volatile unsigned _tail;  \
    struct nblfq_profile_s _profile;                                    \
  } __attribute__ ((aligned (64)));                                     \
                            \
  static inline union ENAME ## _nblfq2_cell_u ENAME ## _nblfq_load(struct ENAME ## _lfqueue_s*q, unsigned index) \
  {                                                                     \
    /* use vmovdqa to read a 128 bit value atomically. Note that it is  \
     * not guaranteed to be atomic in the official documentation, but   \
     * it has been shown to be atomic in practice.                      \
     * https://rigtorp.se/isatomic/                                     \
     * The "official" solution would be the use of CAS2.                \
     */                                                                 \
    /* register __int128 v asm("xmm0");                                 \
    asm("vmovdqa %[source], %[destination]"                             \
        : [destination] "=x" (v)                                        \
        : [source] "m" (q->_array[index].i));                           \
    union ENAME ## _nblfq2_cell_u u = { .i = v };                       \
    return u;    */                                                     \
    /* new version using C11 atomic; should be portable */              \
    union ENAME ## _nblfq2_cell_u u;                                    \
    __atomic_load(&q->_array[index].i, &u.i, __ATOMIC_RELAXED);         \
    return u;                                                           \
  }                                                                     \
                                     \
  static inline unsigned ENAME ## _nblfq_chase_head(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
    unsigned head = q->_head;                                           \
    unsigned prev = (head + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    union ENAME ## _nblfq2_cell_u p = ENAME ## _nblfq_load(q, prev);    \
    union ENAME ## _nblfq2_cell_u u = ENAME ## _nblfq_load(q, head);    \
    for(;;)                                                             \
      {                                                                 \
        /* find an empty cell with a predecessor either non-empty or with a higher seq) */ \
        if(p.cell._value != LFQUEUE_NULL && u.cell._value == LFQUEUE_NULL) \
          return head; /* empty cell with non-empty predecessor: regular head */ \
        if(p.cell._seq > u.cell._seq)                                   \
          { /* out-of-order -> wrap point */                            \
            if(p.cell._value == LFQUEUE_NULL && u.cell._value == LFQUEUE_NULL) \
              return head; /* empty list */                             \
            if(p.cell._value != LFQUEUE_NULL && u.cell._value != LFQUEUE_NULL) \
              { /* list full */                                         \
                q->_head = head;                                        \
                return head;                                            \
              }                                                         \
          }                                                             \
        prev = head;                                                    \
        head = (head + 1) % (NBLFQ_SIZE);                               \
        p = u;                                                          \
        u = ENAME ## _nblfq_load(q, head);                              \
      }                                                                 \
  }                                                                     \
                                     \
  static inline unsigned ENAME ## _nblfq_chase_tail(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
    unsigned tail = q->_tail;                                           \
    unsigned prev = (tail + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    union ENAME ## _nblfq2_cell_u p = ENAME ## _nblfq_load(q, prev);    \
    union ENAME ## _nblfq2_cell_u u = ENAME ## _nblfq_load(q, tail);    \
    while(p.cell._seq < u.cell._seq)                                    \
      {                                                                 \
        prev = tail;                                                    \
        tail = (tail + 1) % (NBLFQ_SIZE);                               \
        p = u;                                                          \
        u = ENAME ## _nblfq_load(q, tail);                              \
      }                                                                 \
    return tail;                                                        \
  }                                                                     \
  __attribute__((unused))                                               \
  static inline void ENAME ## _lfqueue_init(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
    assert(sizeof(TYPE) == sizeof(void*));                              \
    assert((LFQUEUE_NULL) == NULL);                                     \
    int i;                                                              \
    for(i = 0; i < (NBLFQ_SIZE); i++)                                   \
      {                                                                 \
        q->_array[i].cell._value = LFQUEUE_NULL;                        \
        q->_array[i].cell._seq = i;                                     \
      }                                                                 \
    q->_head = 0;                                                       \
    q->_tail = 0;                                                       \
    nblfq_profile_init(&q->_profile, #ENAME);                           \
  }                                                                     \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_enqueue_ext(struct ENAME ## _lfqueue_s*q, ENAME ## _lfqueue_elem_t e, int single) \
  {                                                                     \
    nblfq_profile_inc(q->_profile.n_enqueue);                           \
    int r = 0;                                                          \
  retry:                                                                \
    ;                                                                   \
    /* chase head */                                                    \
    unsigned head = q->_head;                                           \
    unsigned prev = (head + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    union ENAME ## _nblfq2_cell_u p = ENAME ## _nblfq_load(q, prev);;   \
    union ENAME ## _nblfq2_cell_u u = ENAME ## _nblfq_load(q, head);    \
    for(;;)                                                             \
      {                                                                 \
        /* find an empty cell with a predecessor either non-empty or with a higher seq) */ \
        if(p.cell._value != LFQUEUE_NULL && u.cell._value == LFQUEUE_NULL) \
          break; /* empty cell with non-empty predecessor: regular head */ \
        if(p.cell._seq > u.cell._seq)                                   \
          { /* out-of-order -> wrap point */                            \
            if(p.cell._value == LFQUEUE_NULL && u.cell._value == LFQUEUE_NULL) \
              break; /* empty list */                                   \
            if(p.cell._value != LFQUEUE_NULL && u.cell._value != LFQUEUE_NULL) \
              { /* list full */                                         \
                q->_head = head;                                        \
                nblfq_profile_inc(q->_profile.n_enqueue_full);          \
                return -1;                                              \
              }                                                         \
          }                                                             \
        prev = head;                                                    \
        head = (head + 1) % (NBLFQ_SIZE);                               \
        p = u;                                                          \
        u = ENAME ## _nblfq_load(q, head);                              \
      }                                                                 \
    /* compute seq number to use */                                     \
    puk_nblfq2_seq_t s = p.cell._seq + 1;                               \
    if(p.cell._value == LFQUEUE_NULL)                                   \
      { /* list is empty => p = tail */                                 \
        s = s - (NBLFQ_SIZE);                                           \
      }                                                                 \
    if(s != u.cell._seq)                                                \
      { /* inconsistency between p & u tags; retry */                   \
        puk_lfbackoff(r++);                                             \
        __sync_synchronize();                                           \
        goto retry;                                                     \
      }                                                                 \
    /* try to insert value */                                           \
    assert(u.cell._value == LFQUEUE_NULL);                              \
    union ENAME ## _nblfq2_cell_u v; v.cell._value = e; v.cell._seq = s; \
    if(__sync_bool_compare_and_swap((puk_nblfq2_pair_t*)&q->_array[head], u.i, v.i)) \
      {                                                                 \
        q->_head = (head + 1) % (NBLFQ_SIZE);                           \
      }                                                                 \
    else                                                                \
      {                                                                 \
        nblfq_profile_inc(q->_profile.n_enqueue_race);                  \
        puk_lfbackoff(r++);                                             \
        __sync_synchronize();                                           \
        goto retry;                                                     \
      }                                                                 \
    return 0;                                                           \
  }                                                                     \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_enqueue(struct ENAME ## _lfqueue_s*queue, ENAME ## _lfqueue_elem_t e) \
  { return ENAME ## _lfqueue_enqueue_ext(queue, e, 0); }                \
                                                                        \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_enqueue_single_writer(struct ENAME ## _lfqueue_s*queue, ENAME ## _lfqueue_elem_t e) \
  { return ENAME ## _lfqueue_enqueue_ext(queue, e, 1); }                \
                                                                        \
  __attribute__((unused))                                               \
  static inline ENAME ## _lfqueue_elem_t ENAME ## _lfqueue_dequeue_ext(struct ENAME ## _lfqueue_s*q, int single) \
  {                                                                     \
    nblfq_profile_inc(q->_profile.n_dequeue);                           \
    int r = 0;                                                          \
  retry:                                                                \
    ;                                                                   \
    /* chase tail */                                                    \
    unsigned tail = q->_tail;                                           \
    unsigned prev = (tail + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    union ENAME ## _nblfq2_cell_u p = ENAME ## _nblfq_load(q, prev);    \
    union ENAME ## _nblfq2_cell_u u = ENAME ## _nblfq_load(q, tail);    \
    while(p.cell._seq < u.cell._seq)                                    \
      {                                                                 \
        /* find a cell whose predecessor has a higher seq */            \
        prev = tail;                                                    \
        tail = (tail + 1) % (NBLFQ_SIZE);                               \
        p = u;                                                          \
        u = ENAME ## _nblfq_load(q, tail);                              \
      }                                                                 \
    /* check whether queue is empty */                                  \
    if(p.cell._value == LFQUEUE_NULL && u.cell._value == LFQUEUE_NULL)  \
      {                                                                 \
        nblfq_profile_inc(q->_profile.n_dequeue_empty);                 \
        return LFQUEUE_NULL; /* really empty */                         \
      }                                                                 \
    /* compute seq number to use */                                     \
    puk_nblfq2_seq_t s = p.cell._seq + 1;                               \
    union ENAME ## _nblfq2_cell_u v; v.cell._value = LFQUEUE_NULL; v.cell._seq = s; \
    /* try to remove value */                                           \
    if(__sync_val_compare_and_swap((puk_nblfq2_pair_t*)&q->_array[tail], u.i, v.i) == u.i) \
      {                                                                 \
        q->_tail = (tail + 1) % (NBLFQ_SIZE);                           \
        assert(u.cell._value != LFQUEUE_NULL);                          \
        return u.cell._value;                                           \
      }                                                                 \
    else                                                                \
      {                                                                 \
        nblfq_profile_inc(q->_profile.n_dequeue_race);                  \
        puk_lfbackoff(r++);                                             \
        __sync_synchronize();                                           \
        goto retry;                                                     \
      }                                                                 \
    return LFQUEUE_NULL;                                                \
  }                                                                     \
                                                                        \
  __attribute__((unused))                                               \
  static inline ENAME ## _lfqueue_elem_t ENAME ## _lfqueue_dequeue(struct ENAME ## _lfqueue_s*queue) \
  { return ENAME ## _lfqueue_dequeue_ext(queue, 0); }                   \
                                                                        \
  __attribute__((unused))                                               \
  static inline ENAME ## _lfqueue_elem_t ENAME ## _lfqueue_dequeue_single_reader(struct ENAME ## _lfqueue_s*queue) \
  { return ENAME ## _lfqueue_dequeue_ext(queue, 1); }                   \
                       \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_empty(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
    unsigned tail = ENAME ## _nblfq_chase_tail(q);                      \
    /* TODO- do not recompute prev, p, u */                             \
    unsigned prev = (tail + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);           \
    union ENAME ## _nblfq2_cell_u p = q->_array[prev];                  \
    union ENAME ## _nblfq2_cell_u u = q->_array[tail];                  \
    if(p.cell._value == LFQUEUE_NULL && u.cell._value == LFQUEUE_NULL)  \
      return 1;                                                         \
    else                                                                \
      return 0;                                                         \
  }                                                                     \
                                                                   \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_load(struct ENAME ## _lfqueue_s*q) \
  {                                                                     \
  retry:                                                                \
    ;                                                                   \
    unsigned head = q->_head;                                           \
    unsigned tail = q->_tail;                                           \
    if(head != tail)                                                    \
      {                                                                 \
        return (head + (NBLFQ_SIZE) - tail) % (NBLFQ_SIZE);             \
      }                                                                 \
    else                                                                \
      {                                                                 \
        unsigned prev = (tail + (NBLFQ_SIZE) - 1) % (NBLFQ_SIZE);       \
        union ENAME ## _nblfq2_cell_u p = q->_array[prev];              \
        union ENAME ## _nblfq2_cell_u u = q->_array[tail];              \
        if(p.cell._value == LFQUEUE_NULL && u.cell._value == LFQUEUE_NULL) \
          return 0;                                                     \
        else if(p.cell._value != LFQUEUE_NULL && u.cell._value != LFQUEUE_NULL) \
          return (NBLFQ_SIZE);                                          \
        else                                                            \
          { /* inconsistent state; retry */                             \
            q->_head = ENAME ## _nblfq_chase_head(q);                   \
            q->_tail = ENAME ## _nblfq_chase_tail(q);                   \
            goto retry;                                                 \
          }                                                             \
      }                                                                 \
  }                                                                     \
                                   \
  __attribute__((unused))                                               \
  static inline int ENAME ## _lfqueue_size(struct ENAME ## _lfqueue_s*queue __attribute__((unused))) \
  {                                                                     \
    return (NBLFQ_SIZE);                                                \
  }
