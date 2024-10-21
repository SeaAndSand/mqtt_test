#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

typedef struct {
    void* data;
    size_t size;
    pthread_mutex_t lock;
} Cache;

void init_cache(Cache* cache);
void write_cache(Cache* cache, const void* data, size_t size);
void* read_cache(Cache* cache, size_t* size);
void free_cache(Cache* cache);