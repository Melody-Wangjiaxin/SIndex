/*
 * The code is part of the SIndex project.
 *
 *    Copyright (C) 2020 Institute of Parallel and Distributed Systems (IPADS),
 * Shanghai Jiao Tong University. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <immintrin.h>

#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>

#if !defined(HELPER_H)
#define HELPER_H

#define COUT_THIS(this) std::cout << this << std::endl;
#define COUT_VAR(this) std::cout << #this << ": " << this << std::endl;
#define COUT_POS() COUT_THIS("at " << __FILE__ << ":" << __LINE__)
#define COUT_N_EXIT(msg) \
  COUT_THIS(msg);        \
  COUT_POS();            \
  abort();
#define INVARIANT(cond)            \
  if (!(cond)) {                   \
    COUT_THIS(#cond << " failed"); \
    COUT_POS();                    \
    abort();                       \
  }

#if defined(NDEBUGGING)
#define DEBUG_THIS(this) std::cerr << this << std::endl
#else
#define DEBUG_THIS(this) 
#endif

#define UNUSED(var) ((void)var)

#define CACHELINE_SIZE (1 << 6)

#define PACKED __attribute__((packed))

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

inline size_t common_prefix_length(size_t start_i, const uint8_t *key1,
                                   size_t k1_len, const uint8_t *key2,
                                   size_t k2_len) {
  for (size_t f_i = start_i; f_i < std::min(k1_len, k2_len); ++f_i) {
    if (key1[f_i] != key2[f_i]) return f_i - start_i;
  }
  return std::min(k1_len, k2_len) - start_i;
}

inline double dot_product(const double *a, const double *b, size_t len) {
  if (len < 4) {
    double res = 0;
    for (size_t feat_i = 0; feat_i < len; feat_i++) {
      res += a[feat_i] * b[feat_i];
    }
    return res;
  }

  __m256d sum_vec = _mm256_set_pd(0.0, 0.0, 0.0, 0.0);

  for (size_t ii = 0; ii < (len >> 2); ++ii) {
    __m256d x = _mm256_loadu_pd(a + 4 * ii);
    __m256d y = _mm256_loadu_pd(b + 4 * ii);
    // __m256d z = _mm256_mul_pd(x, y);
    // sum_vec = _mm256_add_pd(sum_vec, z);
    sum_vec = _mm256_fmadd_pd(x, y, sum_vec);
  }

  // the partial dot-product for the remaining elements
  double trailing = 0.0;
  for (size_t ii = (len & (~3)); ii < len; ++ii) trailing += a[ii] * b[ii];

  __m256d temp = _mm256_hadd_pd(sum_vec, sum_vec);
  return ((double *)&temp)[0] + ((double *)&temp)[2] + trailing;
}

inline void memory_fence() { asm volatile("mfence" : : : "memory"); }

/** @brief Compiler fence.
 * Prevents reordering of loads and stores by the compiler. Not intended to
 * synchronize the processor's caches. */
inline void fence() { asm volatile("" : : : "memory"); }

inline uint64_t cmpxchg(uint64_t *object, uint64_t expected, uint64_t desired) {
  asm volatile("lock; cmpxchgq %2,%1"
               : "+a"(expected), "+m"(*object)
               : "r"(desired)
               : "cc");
  fence();
  return expected;
}

inline uint8_t cmpxchgb(uint8_t *object, uint8_t expected, uint8_t desired) {
  asm volatile("lock; cmpxchgb %2,%1"
               : "+a"(expected), "+m"(*object)
               : "r"(desired)
               : "cc");
  fence();
  return expected;
}



#endif  // HELPER_H
