#ifndef CXLALLOC_STATIC_H
#define CXLALLOC_STATIC_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Override the default backend. Must be called before `cxlalloc_init`.
 *
 * Backend string must be one of [mmap, shm, cxl].
 * The `destroy` parameter indicates whether the backing file (if it exists)
 * should be deleted after process exit.
 *
 * Note: this is a separate function for backward compatibility.
 */
void cxlalloc_init_backend(const char *backend);

/**
 * Control the global logger filter at runtime.
 *
 * Level string must be one of [off, error, warn, info, debug, trace].
 *
 * This function is thread-safe.
 */
void cxlalloc_set_log(const char *level);

/**
 * Initialize the global CXL allocator.
 *
 * Defaults to the mmap driver if `cxlalloc_init_backend` was not called.
 */
void cxlalloc_init(const char *name,
                   size_t size,
                   uint8_t thread_id,
                   uint8_t thread_count,
                   uint8_t process_id,
                   uint8_t process_count);

bool cxlalloc_is_clean(void);

void cxlalloc_init_thread(size_t thread_id);

void *cxlalloc_malloc(size_t size);

void cxlalloc_link(void *pointer);

void cxlalloc_free(void *pointer);

void cxlalloc_unlink(void *pointer);

void *cxlalloc_realloc(void *pointer, size_t size);

void *cxlalloc_memalign(size_t size, size_t alignment);

void *cxlalloc_get_root(size_t index);

void cxlalloc_set_root(size_t index, void *pointer);

void cxlalloc_close(void);

/**
 * Convert a pointer into the heap in this process address space to a
 * persistent offset that can be used by any process.
 *
 * Returns `true` and writes into `offset` if the pointer points into
 * the heap, or returns `false` and doesn't touch `offset` otherwise.
 *
 * SAFETY: `offset` is 8-byte aligned and can be written to.
 */
bool cxlalloc_pointer_to_offset(const void *pointer, uint64_t *offset);

/**
 * Convert a persistent offset into a pointer in this process address space.
 */
void *cxlalloc_offset_to_pointer(uint64_t offset);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* CXLALLOC_STATIC_H */
