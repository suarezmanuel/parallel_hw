#include <stdio.h>
#include <stdlib.h>
#include "graph.h"
#include <time.h>

#define D 0.15 // damping factor
#define T 8 // thread count
// #define CACHE_LINE_SIZE_FP (int)64/sizeof(float)
#define CACHE_LINE_SIZE_FP 16
#define BLOCK_SIZE (10*CACHE_LINE_SIZE_FP) // cache line count

void initializeRanks(float *ranks, int N) {
    for (int i = 0; i < N; i++) {
        ranks[i] = 1.0 / N;
    }
}

void PageRank(Graph *graph, int iterations, float* ranks) {
    int N = graph->numVertices;
    float *newRanks = (float *)malloc(N * sizeof(float));
    int* outlinkes = (int*)calloc(N, sizeof(int));

    initializeRanks(ranks, N);
    
    //outlinks calculations
    
    for(int i = 0; i < N; i++) {
        node* v = graph->adjacencyListsOut[i];
        while (v!=NULL) {
            outlinkes[i]++;
            v = v->next;
        }
    }
    
    for (int iter = 0; iter < iterations; iter++) {
        //calculate nodes with outlinks to i
        for (int i = 0; i < N; i++) {
            vertex* out2i = (vertex*)calloc(N, sizeof(vertex));
            
            for(int j = 0; j < N; j++) {
                if( j == i) continue;
                node* v = graph->adjacencyListsOut[j];
                while (v != NULL) {
                    if (v->v == i) {
                        out2i[j] = 1;
                        break;
                    }
                    v = v->next;
                }
            }
            
            //calculate i rank
            double sumA = 0.0;
            double sumB = 0.0;
            for(int j = 0 ; j < N; j++) {
                if(out2i[j] == 1) {
                    sumA += ranks[j]/outlinkes[j];
                } else if(outlinkes[j] == 0) {
                    sumB += ranks[j]/N;
                }
            }
            newRanks[i] = D/N +(1-D)*(sumA+sumB);
        }

        for (int i = 0; i < N; i++) {
            ranks[i] = newRanks[i];
        }
    }

    free(newRanks);
}

void GoodPageRank(Graph *graph, int iterations, float* ranks) {

    int N = graph->numVertices;
    float *newRanks = (float *)malloc(N * sizeof(float));
    initializeRanks(ranks, N);

    for (int iter = 0; iter < iterations; iter++) {

        double sumB = 0.0;
        // calculate the sum of ranks of those without outlinks
        for (int i=0; i < N; i++) {
            sumB += (graph->adjacencyListsOutLength[i] == 0) ? ranks[i]/N : 0;
        }

        //calculate nodes with outlinks to i
        for (int i = 0; i < N; i++) {
            //calculate i rank
            double sumA = 0.0;
            node* u = graph->adjacencyListsIn[i];
            while (u != NULL) {
                // u->v is the id
                sumA += ranks[u->v]/graph->adjacencyListsOutLength[u->v];
                u = u->next;
            }
            newRanks[i] = D/N +(1-D)*(sumA+sumB);
        }

        for (int i = 0; i < N; i++) {
            ranks[i] = newRanks[i];
        }
    }

    free(newRanks);
}

void help (Graph* graph, float* ranks, float* newRanks, int start, int end, double sumB, int N) {
    
    for (int i=start; i < end; i++) {
        //calculate i rank
        double sumA = 0.0;
        node* u = graph->adjacencyListsIn[i];
        while (u != NULL) {
            // u->v is the id
            sumA += ranks[u->v]/graph->adjacencyListsOutLength[u->v];
            u = u->next;
        }
        newRanks[i] = D/N +(1-D)*(sumA+sumB);
    }
}

typedef struct ThreadData {
    Graph* graph;
    float* ranks;
    float* newRanks;
    int start;
    int end;
    double sumB;
    int N;
} ThreadData;

typedef struct Task {
    ThreadData* data;
    struct Task* next;
} Task;

typedef struct TaskQueue {
    Task* front;
    Task* back;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} TaskQueue;

typedef struct ThreadPool {
    // how many to join
    int thread_count;
    int stop;
    pthread_t* threads;
    TaskQueue queue;
    int pending_tasks;
    pthread_mutex_t lock;
    pthread_cond_t done;
} ThreadPool;

void initQueue (TaskQueue* queue) {
    queue->front = queue->back = NULL;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond, NULL);
}

void enqueue (ThreadPool* pool, ThreadData* data) {
    Task* task = malloc(sizeof(Task));
    task->data = data;
    task->next = NULL;

    pthread_mutex_lock(&pool->queue.mutex);
    if (pool->queue.back == NULL) {
        pool->queue.front = pool->queue.back = task;
    } else {
        pool->queue.back->next = task;
        pool->queue.back = task;
    }
    pthread_mutex_unlock(&pool->queue.mutex);

    pthread_mutex_lock(&pool->queue.mutex);
    pool->pending_tasks++;
    pthread_mutex_unlock(&pool->queue.mutex);

    // signal new task
    pthread_cond_signal(&pool->queue.cond);
}

ThreadData* dequeue (ThreadPool* pool) {

    pthread_mutex_lock(&pool->queue.mutex);
    while (pool->queue.front == NULL && !pool->stop) {
        // wait for available task
        pthread_cond_wait(&pool->queue.cond, &pool->queue.mutex);
    }

    if (pool->stop) {
        // stop here
        pthread_mutex_unlock(&pool->queue.mutex);
        return NULL;
    }

    Task* task = pool->queue.front;
    pool->queue.front = pool->queue.front->next;
    if (pool->queue.front == NULL) {
        pool->queue.back = NULL;
    }
    pthread_mutex_unlock(&pool->queue.mutex);

    ThreadData* data = task->data;
    free(task);
    return data;
}


void* help2 (ThreadData* data) {

    for (int i=data->start; i < data->end; i++) {
        //calculate i rank
        double sumA = 0.0;
        node* u = data->graph->adjacencyListsIn[i];
        while (u != NULL) {
            // u->v is the id
            sumA += data->ranks[u->v]/data->graph->adjacencyListsOutLength[u->v];
            u = u->next;
        }
        data->newRanks[i] = D/data->N +(1-D)*(sumA+data->sumB);
    }
    return NULL;
}

void* worker_thread (void* arg) {
    ThreadPool* pool = (ThreadPool*)arg;
    while (1) {
        // sleeps thread until task available
        ThreadData* data = dequeue(pool);
        if (data == NULL) {
            break; // error
        }
        help2(data);

        pthread_mutex_lock(&pool->queue.mutex);
        pool->pending_tasks--;
        if (pool->pending_tasks == 0) {
            pthread_cond_signal(&pool->done);
        }
        pthread_mutex_unlock(&pool->queue.mutex);
    }
    return NULL; // error
}

void initPool(ThreadPool* pool, int thread_count) {
    pool->thread_count = thread_count;
    pool->stop = 0;
    pool->threads = malloc(thread_count * sizeof(pthread_t));
    if (!pool->threads) {
        perror("failed to allocate threads");
        exit(EXIT_FAILURE);
    }

    for (int i=0; i < thread_count; i++) {
        if (pthread_create(&pool->threads[i], NULL, worker_thread, (void*)pool)) {
            perror("failed to initialize threads");
            exit(EXIT_FAILURE);
        }
    }

    initQueue(&pool->queue);
    pool->pending_tasks = 0;
    pthread_mutex_init(&pool->lock, NULL);
    pthread_cond_init(&pool->done, NULL);
}

void destroyPool(ThreadPool* pool) {
    // wait for task to end
    pthread_mutex_lock(&pool->queue.mutex);
    pool->stop = 1;
    // wake sleeping threads to stop
    pthread_cond_broadcast(&pool->queue.cond);
    pthread_mutex_unlock(&pool->queue.mutex);

    for (int i=0; i < pool->thread_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }

    free(pool->threads);
    pthread_mutex_destroy(&pool->queue.mutex);
    pthread_cond_destroy(&pool->queue.cond);
    pthread_mutex_destroy(&pool->lock);
    pthread_cond_destroy(&pool->done);
}

void ParallelPageRank(Graph *graph, int iterations, float* ranks) {

    int N = graph->numVertices;
    float *newRanks = (float *)malloc(N * sizeof(float));

    // calc ceil
    int task_count = (N+BLOCK_SIZE-1) / BLOCK_SIZE;
    int step = N / task_count;
    int mod = N % task_count;
    int sub = N - mod;

    ThreadPool* pool = (ThreadPool*) malloc (sizeof(ThreadPool));
    if (!pool) {
        perror("failed to allocate threadpool");
        exit(EXIT_FAILURE);
    }
    initPool(pool, T);
    ThreadData** data = malloc(task_count * sizeof(ThreadData*));
    for (int i=0; i < task_count; i++) {
        data[i] = (ThreadData*) malloc(sizeof(ThreadData));
    }

    initializeRanks(ranks, N);

    for (int iter = 0; iter < iterations; iter++) {

        double sumB = 0.0;
       
        // calculate the sum of ranks of those without outlinks
        for (int i=0; i < N; i++) {
            sumB += (graph->adjacencyListsOutLength[i] == 0) ? ranks[i] : 0;
        }
        sumB /= N;

        int start = 0, end = 0;
        for (int i=0; i < task_count; i++) {
            end += ((i==task_count-1 && mod != 0) ? mod : step);

            data[i]->graph=graph; data[i]->ranks=ranks; data[i]->newRanks=newRanks; data[i]->sumB=sumB; data[i]->N=N;
            data[i]->start = start; data[i]->end = end;
            enqueue(pool, data[i]);

            start = end;
        }

        pthread_mutex_lock(&pool->lock);
        while (pool->queue.front != NULL) {
            // waits until pool->done signal, releases lock to later reacquire
            pthread_cond_wait(&pool->done, &pool->lock);
        };
        pthread_mutex_unlock(&pool->lock);

        // pointer swapping instead of assignment
        float* temp = ranks;
        ranks = newRanks; 
        newRanks = temp;
    }

    destroyPool(pool);
    free(pool);
    for (int i=0; i < task_count; i++) free(data[i]);
    free(data);
    free(newRanks);
}

void generateRandomGraph(Graph* graph, int N, int M) {
    srand(time(NULL));
    for(int i = 0; i < M; i++) {
        int src = rand() % N;
        int dest = rand() % N;
        if(src != dest) { // Avoid self-loops
            addEdge(graph, src, dest);
        }
    }
}

int main(void) {
    int N = 1000; // number of nodes
    int iterations = 100; // number of iterations

    // Initialize the graph
    Graph *graph = createGraph(N);
    generateRandomGraph(graph, N, 10000);
    
    // Calculate PageRank
    float *ranks = (float *)malloc(N * sizeof(float));
    double start, end, total=0;

    start = (double)clock() / CLOCKS_PER_SEC;
    PageRank(graph, iterations, ranks);
    end   = (double)clock() / CLOCKS_PER_SEC;
    total += (end-start);
    
    // Print the ranks
    // for (int i = 0; i < N; i++) {
    //     printf("Serial - Rank of node %d: %f\n", i, ranks[i]);
    // }
    printf("time to calc: %lf\n", total);

    total = 0;
    start = (double)clock() / CLOCKS_PER_SEC;
    GoodPageRank(graph, iterations, ranks);
    end   = (double)clock() / CLOCKS_PER_SEC;
    total += (end-start);

    // Print the ranks
    // for (int i = 0; i < N; i++) {
    //     printf("Good - Rank of node %d: %f\n", i, ranks[i]);
    // }
    printf("time to calc: %lf\n", total);
    

    total = 0;
    start = (double)clock() / CLOCKS_PER_SEC;
    ParallelPageRank(graph, iterations, ranks);
    end   = (double)clock() / CLOCKS_PER_SEC;
    total += (end-start);

    // Print the ranks
    // for (int i = 0; i < N; i++) {
    //     printf("Parallel - Rank of node %d: %f\n", i, ranks[i]);
    // }
    printf("time to calc: %lf\n", total);

    // Free allocated memory
    for (int i = 0; i < N; i++) {
        node *adjList = graph->adjacencyListsOut[i];
        while (adjList != NULL) {
            node *temp = adjList;
            adjList = adjList->next;
            free(temp);
        }
    }
    

    free(ranks);
    free(graph->adjacencyListsOut);
    free(graph);

    return 0;
}
