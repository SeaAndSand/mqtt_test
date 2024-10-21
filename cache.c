#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "cache.h"

void init_cache(Cache* cache) {
    cache->data = NULL;
    cache->size = 0;
    pthread_mutex_init(&cache->lock, NULL);
}

void write_cache(Cache* cache, const void* data, size_t size) {
    pthread_mutex_lock(&cache->lock);
    free(cache->data);
    cache->data = malloc(size);
    memcpy(cache->data, data, size);
    cache->size = size;
    pthread_mutex_unlock(&cache->lock);
}

void* read_cache(Cache* cache, size_t* size) {
    pthread_mutex_lock(&cache->lock);
    void* data = NULL;
    if (cache->data) {
        data = malloc(cache->size);
        memcpy(data, cache->data, cache->size);
        *size = cache->size;
        free(cache->data);
        cache->data = NULL;
        cache->size = 0;
    }
    pthread_mutex_unlock(&cache->lock);
    return data;
}

void free_cache(Cache* cache) {
    pthread_mutex_lock(&cache->lock);
    free(cache->data);
    cache->data = NULL;
    cache->size = 0;
    pthread_mutex_unlock(&cache->lock);
    pthread_mutex_destroy(&cache->lock);
}