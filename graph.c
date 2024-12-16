#ifndef GRAPH_H
#define GRAPH_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

typedef int vertex;
typedef pthread_mutex_t mutex;

struct node {
    vertex v;
    struct node *next;
};

typedef struct node node;

struct Graph {
    unsigned int numVertices;
    node **adjacencyListsOut;   
    int* adjacencyListsOutLength;
    node **adjacencyListsIn;
    int* adjacencyListsInLength;
};

typedef struct Graph Graph;

// Function to create a node
node *createNode(vertex v) {
    node *newNode = (node *)malloc(sizeof(node));
    if (!newNode) {
        printf("Memory allocation failed\n");
        exit(1);
    }
    newNode->v = v;
    newNode->next = NULL;
    return newNode;
}

// Function to create a graph with given vertices
Graph *createGraph(int vertices) {
    Graph *graph = (Graph *)malloc(sizeof(Graph));
    if (!graph) {
        printf("Memory allocation failed\n");
        exit(1);
    }
    graph->numVertices = vertices;

    graph->adjacencyListsInLength  = (int*) calloc(vertices, sizeof(int));
    graph->adjacencyListsOutLength = (int*) calloc(vertices, sizeof(int));
    
    graph->adjacencyListsOut = (node **)malloc(vertices * sizeof(node *));
    if (!graph->adjacencyListsOut) {
        printf("Memory allocation failed\n");
        free(graph);
        exit(1);
    }

    graph->adjacencyListsIn = (node **)malloc(vertices * sizeof(node *));
    if (!graph->adjacencyListsIn) {
        printf("Memory allocation failed\n");
        free(graph);
        exit(1);
    }

    for (int i = 0; i < vertices; i++) {
        graph->adjacencyListsOut[i] = NULL;
    }

    for (int i = 0; i < vertices; i++) {
        graph->adjacencyListsIn[i] = NULL;
    }

    return graph;
}

void addEdge(Graph *graph, vertex source, vertex destination) {
    // Outbound edge
    node *newNodeOut = createNode(destination);
    newNodeOut->next = graph->adjacencyListsOut[source];
    graph->adjacencyListsOut[source] = newNodeOut;
    graph->adjacencyListsOutLength[source]++;

    // Inbound edge
    node *newNodeIn = createNode(source);
    newNodeIn->next = graph->adjacencyListsIn[destination];
    graph->adjacencyListsIn[destination] = newNodeIn;
    graph->adjacencyListsInLength[destination]++;
}


#endif
