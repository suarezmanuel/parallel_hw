#include <stdio.h>
#include <stdlib.h>
#include "graph.h"
#include <time.h>

#define N 10  // node count
#define M 20  // edge count
#define D 0.15 // damping factor
#define T 8    // thread count
#define I 100  // iterations count
// should be 16
#define BLOCK_SIZE (64 / sizeof(float))

typedef struct __attribute__((aligned(64))) e {
    float data [BLOCK_SIZE];
} e;

// init ranks to 1/N
void initializeRanks(float* ranks) {
    for (int i = 0; i < N; i++) ranks[i] = 1.0 / N;
}

void printRanks(float* ranks) {
    for (int i = 0; i < N; i++) printf("%f \n", ranks[i]);
    printf("\n");
}

void initializeRanksStruct(e* ranks) {
    for (int i = 0; i < N; i++) {
        ranks[i/BLOCK_SIZE].data[i] = 1.0 / N;
    }
}

void printRanksStruct(e* ranks) {
    int ceil = (N + BLOCK_SIZE - 1) / BLOCK_SIZE;
    for (int i = 0; i < ceil; i++) {
        for (int j=0; j < BLOCK_SIZE; j++) {
            printf("%f \n", ranks[i].data[j]);
        }
    }
    printf("\n");
}

void PageRank(Graph *graph, float* ranks) {

    float *newRanks = (float *)malloc(N * sizeof(float));
    int* outlinkes = (int*)calloc(N, sizeof(int));

    initializeRanks(ranks);
    
    //outlinks calculations
    
    for(int i = 0; i < N; i++) {
        node* v = graph->adjacencyListsOut[i];
        while (v!=NULL) {
            outlinkes[i]++;
            v = v->next;
        }
    }
    
    for (int iter = 0; iter < I; iter++) {
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

void GoodPageRank(Graph *graph, float* ranks) {

    float *newRanks = (float *)malloc(N * sizeof(float));
    initializeRanks(ranks);

    for (int iter = 0; iter < I; iter++) {

        double sumB = 0.0;
        // calculate the sum of ranks of those without outlinks
        for (int i=0; i < N; i++) {
            // dont divide outside for precision
            sumB += (graph->adjacencyListsOutLength[i] == 0) ? ranks[i]/N : 0;
        }

        // calculate nodes with outlinks to i
        for (int i = 0; i < N; i++) {
            // calculate i rank
            double sumA = 0.0;
            node* u = graph->adjacencyListsIn[i];
            while (u != NULL) {
                // u->v is the id
                sumA += ranks[u->v]/graph->adjacencyListsOutLength[u->v];
                u = u->next;
            }
            newRanks[i] = D/N +(1-D)*(sumA+sumB);
        }

        // pointer switching instead of slow assignment
        float* temp = newRanks;
        newRanks = ranks;
        ranks = temp;
    }

    free(newRanks);
}

void ParallelPageRank(Graph *graph, float *ranks)
{
    float *newRanks = (float *)malloc(N * sizeof(float));
    initializeRanks(ranks);

    for (int iter = 0; iter < I; iter++) {

        double sumB = 0.0;
        // calculate the sum of ranks of those without outlinks
        for (int i=0; i < N; i++) {
            // dont divide outside for precision
            sumB += (graph->adjacencyListsOutLength[i] == 0) ? ranks[i]/N : 0;
        }

        // calculate nodes with outlinks to i
        for (int i = 0; i < N; i++) {
            // calculate i rank
            double sumA = 0.0;
            node* u = graph->adjacencyListsIn[i];
            while (u != NULL) {
                // u->v is the id
                sumA += ranks[u->v]/graph->adjacencyListsOutLength[u->v];
                u = u->next;
            }
            newRanks[i] = D/N +(1-D)*(sumA+sumB);
        }

        // pointer switching instead of slow assignment
        float* temp = newRanks;
        newRanks = ranks;
        ranks = temp;
    }

    free(newRanks);
}

void benchmark(Graph* graph, void (*f)(Graph*, float*), char* name) {
    float *ranks = (float *)calloc(N, sizeof(float));
    double start, end, total = 0;

    start = (double)clock() / CLOCKS_PER_SEC;

    f(graph, ranks);

    end = (double)clock() / CLOCKS_PER_SEC;
    total += (end - start);
    // Print the ranks
    // for (int i = 0; i < N; i++) { printf("Rank of node %d: %f\n", i, ranks[i]); }
    printf("time to calc %-9s  \e[1m%lf\e[m\n", name, total);   
    free(ranks);
}

void compare(Graph* graph, void (*f1)(Graph*, float*), void (*f2)(Graph*, float*), char* name1, char* name2) {
    float *ranks1 = (float *)calloc(N, sizeof(float));
    float *ranks2 = (float *)calloc(N, sizeof(float));

    f1(graph, ranks1);
    f2(graph, ranks2);

    int same = 1;
    for (int i=0; i < N; i++) {
        if (ranks1[i] != ranks2[i]) same = 0;
    }
    // some terminal sugar
    printf("%-6s and %-8s are \e[1m%s\e[m\n", name1, name2, (same == 0 ? "different" : "equal"));
    free(ranks1); free(ranks2);
}

void compare2(Graph* graph, float (*f1)(Graph*), float (*f2)(Graph*), char* name1, char* name2) {

    float s1 = f1(graph);
    float s2 = f2(graph);

    int same = (s1 == s2);
    // some terminal sugar
    printf("%-6s and %-8s are \e[1m%s\e[m\n", name1, name2, (same == 0 ? "different" : "equal"));
}

// function to generate graph of a given size without duplicate edges
void generateRandomGraph(Graph *graph) {
    srand(time(NULL));
    // prob per edge to get avg M/N
    double p = (double)M / (N * (N - 1));

    for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
            // add edge with prob p
            if (i != j && ((double)rand() / RAND_MAX) < p) {
                addEdge(graph, i, j);
            }
        }
    }
}

float t1 (Graph* graph) {
    float* ranks = (float*) calloc(N, sizeof(float));
    float sumB = 0.0;
    initializeRanks(ranks);
    printRanks(ranks);

    for (int iter = 0; iter < I; iter++) 
        // calculate the sum of ranks of those without outlinks
        for (int i=0; i < N; i++) 
            // dont divide outside for precision
            sumB += (graph->adjacencyListsOutLength[i] == 0) ? ranks[i]/N : 0;

    return sumB;
}

float t2 (Graph* graph) {
    // ceil(N/BLOCK_SIZE) structs
    int ceil = (N + BLOCK_SIZE - 1) / BLOCK_SIZE;
    e* ranks = calloc(ceil, sizeof(e));;

    float sumB = 0.0;
    initializeRanksStruct(ranks);
    // printRanksStruct(ranks);

    for (int iter = 0; iter < I; iter++) 
        // calculate the sum of ranks of those without outlinks
        for (int i=0; i < ceil; i++) {
            for (int j=0; j < (BLOCK_SIZE*i + N % BLOCK_SIZE) && (j < BLOCK_SIZE); j++) {
                // dont divide outside for precision
                sumB += (graph->adjacencyListsOutLength[i] == 0) ? ranks[i].data[j]/N : 0;
            }
        }

    return sumB;
}

int main()
{
    // Initialize the graph
    Graph *graph = createGraph(N);
    generateRandomGraph(graph);
    
    printf("\n");
    benchmark(graph, PageRank, "serial");
    benchmark(graph, GoodPageRank, "good");
    benchmark(graph, ParallelPageRank, "parallel");
    printf("\n");
    compare(graph, PageRank, GoodPageRank, "serial", "good");
    compare(graph, GoodPageRank, ParallelPageRank, "good", "parallel");
    printf("\n");
    compare2(graph, t1, t2, "sum-serial", "sum-parallel");
    printf("\n");

    // Free allocated memory
    for (int i = 0; i < N; i++)
    {
        node* adjList = graph->adjacencyListsOut[i];
        while (adjList != NULL)
        {
            node* temp = adjList;
            adjList = adjList->next;
            free(temp);
        }

        adjList = graph->adjacencyListsIn[i];
        while (adjList != NULL)
        {
            node* temp = adjList;
            adjList = adjList->next;
            free(temp);
        }
    }

    free(graph->adjacencyListsOut);
    free(graph->adjacencyListsIn);
    free(graph);

    return 0;
}
