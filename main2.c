#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include "graph.h"
#include <time.h>

#define D 0.15 // damping factor
#define T 8    // thread count
#define CACHE_LINE_SIZE_FP 16
#define BLOCK_SIZE (10 * CACHE_LINE_SIZE_FP) // cache line count

// #define _POSIX_BARRIERS 1
pthread_barrier_t barrier; // Barrier for synchronization

typedef struct ThreadData {
    Graph* graph;
    float* ranks;
    float* newRanks;
    int start;
    int end;
    double sumB;
    int N;
} ThreadData;

typedef struct ThreadPool {
    int thread_count;
    pthread_t* threads;
    ThreadData* thread_data;
    int iterations;
} ThreadPool;

// Function to compute partial ranks for a segment
void computePartialRanks(ThreadData* data) {
    for (int i = data->start; i < data->end; i++) {
        double sumA = 0.0;
        node* u = data->graph->adjacencyListsIn[i];
        while (u != NULL) {
            sumA += data->ranks[u->v] / data->graph->adjacencyListsOutLength[u->v];
            u = u->next;
        }
        data->newRanks[i] = D / data->N + (1 - D) * (sumA + data->sumB);
    }
}

// Worker thread function
void* worker_thread(void* arg) {
    ThreadPool* pool = (ThreadPool*)arg;
    // Determine thread index
    pthread_t self = pthread_self();
    int thread_idx = -1;
    for (int i = 0; i < pool->thread_count; i++) {
        if (pthread_equal(self, pool->threads[i])) {
            thread_idx = i;
            break;
        }
    }
    if (thread_idx == -1) {
        fprintf(stderr, "Thread not found in pool\n");
        pthread_exit(NULL);
    }

    ThreadData* data = &pool->thread_data[thread_idx];

    for (int iter = 0; iter < pool->iterations; iter++) {
        // Wait for main thread to set sumB
        pthread_barrier_wait(&barrier);

        // Compute partial ranks
        computePartialRanks(data);

        // Wait for all threads to finish computation
        pthread_barrier_wait(&barrier);
    }
    return NULL;
}

void ParallelPageRank(Graph *graph, int iterations, float* ranks) {
    int N = graph->numVertices;
    float *newRanks = (float *)malloc(N * sizeof(float));
    if (!newRanks) {
        perror("Failed to allocate newRanks");
        exit(EXIT_FAILURE);
    }

    // Initialize the barrier with T worker threads + 1 main thread
    if (pthread_barrier_init(&barrier, NULL, T + 1)) {
        fprintf(stderr, "Could not create a barrier\n");
        exit(EXIT_FAILURE);
    }

    // Initialize thread pool
    ThreadPool pool;
    pool.thread_count = T;
    pool.iterations = iterations;
    pool.threads = malloc(T * sizeof(pthread_t));
    pool.thread_data = malloc(T * sizeof(ThreadData));
    if (!pool.threads || !pool.thread_data) {
        perror("Failed to allocate threads or thread_data");
        exit(EXIT_FAILURE);
    }

    // Determine the workload for each thread
    int chunk_size = (N + T - 1) / T; // Ceiling division

    for (int i = 0; i < T; i++) {
        pool.thread_data[i].graph = graph;
        pool.thread_data[i].ranks = ranks;
        pool.thread_data[i].newRanks = newRanks;
        pool.thread_data[i].sumB = 0.0; // Will be computed each iteration
        pool.thread_data[i].N = N;
        pool.thread_data[i].start = i * chunk_size;
        pool.thread_data[i].end = (i + 1) * chunk_size;
        if (pool.thread_data[i].end > N) pool.thread_data[i].end = N;

        if (pthread_create(&pool.threads[i], NULL, worker_thread, (void*)&pool)) {
            perror("Failed to create thread");
            exit(EXIT_FAILURE);
        }
    }

    initializeRanks(ranks, N); // Initialize ranks

    for (int iter = 0; iter < iterations; iter++) {
        double sumB = 0.0;

        // Calculate the sum of ranks of nodes without outlinks
        for (int i = 0; i < N; i++) {
            if (graph->adjacencyListsOutLength[i] == 0) {
                sumB += ranks[i];
            }
        }
        sumB /= N;

        // Assign sumB to each thread's data
        for (int i = 0; i < T; i++) {
            pool.thread_data[i].sumB = sumB;
        }

        // First barrier: main thread waits for all worker threads to reach this point
        pthread_barrier_wait(&barrier);

        // Swap ranks
        float* temp = ranks;
        ranks = newRanks;
        newRanks = temp;

        // Update ranks pointer in thread data for next iteration
        for (int i = 0; i < T; i++) {
            pool.thread_data[i].ranks = ranks;
            pool.thread_data[i].newRanks = newRanks;
        }

        // Second barrier: wait for worker threads to finish computation
        pthread_barrier_wait(&barrier);
    }

    // Join threads
    for (int i = 0; i < T; i++) {
        pthread_join(pool.threads[i], NULL);
    }

    pthread_barrier_destroy(&barrier);
    free(pool.threads);
    free(pool.thread_data);
    free(newRanks);
}
