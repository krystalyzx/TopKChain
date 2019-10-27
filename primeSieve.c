#include <stdio.h>  
#include <stdlib.h> 
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <mpi.h>

/* forward declarations */

int sieveElement(int argc, char **argv);
int generator(int argc, char **argv);
int who_are_my_neighbors(int rank, int size, int *prevproc_ptr, int *nextproc_ptr);

#define MAXINT 1000000
#define FALSE 0
#define TRUE !FALSE

#define DATA_TAG 111
#define STOP_TAG 999

/* A temporary shared array that holds the random
   permutation of the processes created by random mapper.
   This array is only read by the co-located processes
   once to discover their previous and next neighbors
   and is de-allocated after that */  
int *proc = NULL;

/******* FG-MPI Boilerplate begin *********/
#include "fgmpi.h"
/* forward declarations */
FG_ProcessPtr_t sequential_mapper(int argc, char** argv, int rank);
FG_ProcessPtr_t random_mapper(int argc, char** argv, int rank);

FG_MapPtr_t map_lookup(int argc, char** argv, char* str)
{ 

    /* return default mapper if FGMAP environment variable is not specified */
    return (&sequential_mapper); 
}

int main( int argc, char *argv[] )
{
    FGmpiexec(&argc, &argv, &map_lookup);
    
    return (0);
}


/******* Batched version *********/


//initialize all entries to INT_MAX
void initMinArray(int* array, int count){
    int i;
    for(i = 0;i<count;i++){
        array[i] = INT_MAX;
    }
}
void printMinNums(int rank, int* array, int count){
    int i;
    for(i = 0;i<count;i++){
        printf("%d: %d\n", rank, array[i]);
    }
}

//for sorting in an increasing order
int cmpfunc (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}

int sieveElementBatch(int argc, char **argv)
{
    int nextproc, prevproc;
    int rank, size;
    MPI_Status status;
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    who_are_my_neighbors(rank, size, &prevproc, &nextproc); 
    

    int batchSize = atoi(argv[2]);
    int myminsSize = batchSize * 2;
    
    //first half of the array contains the smallest values seen so far
    //second half of the array is used to send and receive data 
    int* mymins = malloc(myminsSize*sizeof(int));  
    initMinArray(mymins, myminsSize);
    int* nums = mymins + batchSize;

    while (1){
        MPI_Recv(nums,batchSize,MPI_INT,prevproc,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        if ( status.MPI_TAG == DATA_TAG ){
            //sort the array in an increasing order
            qsort(mymins, myminsSize, sizeof(int), cmpfunc); 
            //pass on the largest batchSize integers
            if(rank != size -1){
                MPI_Send(nums ,batchSize ,MPI_INT,nextproc,DATA_TAG,MPI_COMM_WORLD); 
            }
		} else if ( status.MPI_TAG == STOP_TAG ){
            //print the smallest values seen so far and send stop signal
            printMinNums(rank, mymins, batchSize); 
            memset(nums, 0, batchSize*sizeof(int));
            if(rank != size - 1){
                MPI_Send(nums, batchSize,MPI_INT,nextproc,STOP_TAG,MPI_COMM_WORLD);
            }
            break;
		} else{
            fprintf(stderr, "ERROR ERROR bad TAG \n");
            MPI_Abort(MPI_COMM_WORLD,rank);
		}
    }
    
    MPI_Finalize();
    free(mymins);
    return 0;
}	


int generatorBatch(int argc, char **argv)
{
    int nextproc, prevproc;
    int rank, size;
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Request request;
    MPI_Status  status;
    who_are_my_neighbors(rank, size, &prevproc, &nextproc);
    
    if(argc < 3){
        fprintf(stderr, "Not enough arguments\n");
        MPI_Abort(MPI_COMM_WORLD,rank);
    }

    char* fname = argv[1];
    int batchSize = atoi(argv[2]);

    FILE *fp = fopen(fname, "r");
    if(fp == NULL){
        fprintf(stderr, "Unable to open file\n");
        MPI_Abort(MPI_COMM_WORLD,rank);
    }

    //count the number of integers in the file
    char c;
    int count = 0;
    for (c = getc(fp); c != EOF; c = getc(fp)){
        if (c == '\n'){
            count = count + 1;
        }
    }
    printf("%d integers in the file\n", count);
    fseek(fp, 0, SEEK_SET);
    
    if(count < size * batchSize){
        fprintf(stderr, "Too few integers\n");
        MPI_Abort(MPI_COMM_WORLD,rank);
    }

    int myminsSize = batchSize * 2;
    int* mymins = malloc(myminsSize*sizeof(int)); 
    initMinArray(mymins, myminsSize);
    int* nums = mymins + batchSize;
    char* buf = calloc(1024,1);
    int bsize = 1024;
    int eof = 0;
    int i;
    while(!eof){
        //read next batch of values
        for(i = 0; i < batchSize; i++){
            if(fgets(buf,bsize,fp)!= NULL){
                nums[i] = atoi(buf);
            }else{
                nums[i] = INT_MAX;
                eof = 1;
            }
        }
         qsort(mymins, myminsSize, sizeof(int), cmpfunc); //sort the array in increasing order
         if(rank != size - 1){
             MPI_Send(nums ,batchSize ,MPI_INT,nextproc,DATA_TAG,MPI_COMM_WORLD);
         }
    }
    //print the minimum integer
    printMinNums(rank, mymins, batchSize);

    //send stop signal and finalize
    memset(nums, 0, batchSize*sizeof(int));
    if(rank != size -1){
        MPI_Send(nums, batchSize,MPI_INT,nextproc,STOP_TAG,MPI_COMM_WORLD);
    }
    MPI_Finalize();
    free(mymins);
    free(buf);
    return 0;
}

/******* Basic version *********/

int sieveElement(int argc, char **argv)
{
    int nextproc, prevproc;
    int rank, size;
    MPI_Status status;
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    who_are_my_neighbors(rank, size, &prevproc, &nextproc); 
    
    int num,temp;
    int mymin = INT_MAX;
    while (1){
        MPI_Recv(&temp,1,MPI_INT,prevproc,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        if ( status.MPI_TAG == DATA_TAG ){
            if ( temp < mymin ){
                num = mymin;    //send the old value and keep the new value
                mymin = temp;
	  		}else{
                num = temp;     //send the new value and keep the old value           
	  		}
            if(rank != size -1){
                 MPI_Send(&num,1,MPI_INT,nextproc,DATA_TAG,MPI_COMM_WORLD);
            }
		} else if ( status.MPI_TAG == STOP_TAG ){
            printf("%d: %d\n", rank, mymin);
            if(rank != size -1){
                num=1;
                MPI_Send(&num,1,MPI_INT,nextproc,STOP_TAG,MPI_COMM_WORLD);
            }
            break;
		} else{
            fprintf(stderr, "ERROR ERROR bad TAG \n");
            MPI_Abort(MPI_COMM_WORLD,rank);
		}
    }

    MPI_Finalize();
    return 0;
}	



int generator(int argc, char **argv)
{
    int nextproc, prevproc;
    int rank, size;
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Request request;
    MPI_Status  status;
    who_are_my_neighbors(rank, size, &prevproc, &nextproc);

    char* fname = argv[1];
    FILE *fp = fopen(fname, "r");
    if(fp == NULL){
        fprintf(stderr, "Unable to open file\n");
        MPI_Abort(MPI_COMM_WORLD,rank);
      
    }

    //count the number of integers in the file
    char c;
    int count = 0;
    for (c = getc(fp); c != EOF; c = getc(fp)){
        if (c == '\n'){
            count = count + 1;
        }
    }
    printf("%d integers in the file\n", count);
    fseek(fp, 0, SEEK_SET);
    
    if(count < size){
        fprintf(stderr, "Too few integers\n");
        MPI_Abort(MPI_COMM_WORLD,rank);
    }

    int mymin = INT_MAX;
    int num;
    char* buf = calloc(1024,1);
    int bsize = 1024;
    while (fgets(buf,bsize,fp)!= NULL){
        int temp = atoi(buf);
        if(temp < mymin){
            num = mymin;        //send the old value and keep the new value
            mymin = temp;
        }else{
            num = temp;         //send the new value and keep the old value
        }
        if(rank != size -1){
            MPI_Send(&num,1,MPI_INT,nextproc,DATA_TAG,MPI_COMM_WORLD);
        }
        
    }
    //print the minimum integer
    printf("%d: %d\n", rank, mymin);

    //send stop signal and finalize
    num = 0;
    if(rank != size -1){
        MPI_Send(&num,1,MPI_INT,nextproc,STOP_TAG,MPI_COMM_WORLD);
    }
    MPI_Finalize();
    free(buf);
    return 0;
}




FG_ProcessPtr_t sequential_mapper(int argc, char** argv, int rank)
{
    int worldsize;
    MPI_Comm_size(MPI_COMM_WORLD, &worldsize);
    
    if ( (rank == MAP_INIT_ACTION) || (rank == MAP_FINALIZE_ACTION) ) 
        return (NULL);
    if(atoi(argv[2]) == 1){
        if ( 0 == rank ) return (&generator);  
        return (&sieveElement);
    }
    if ( 0 == rank ) return (&generatorBatch);  
    return (&sieveElementBatch);
}


int who_are_my_neighbors(int rank, int size, int *prevproc_ptr, int *nextproc_ptr)
{
    int prevproc = -1, nextproc = -1;
    char *mapstr = getenv("FGMAP");
    
    if ( mapstr && !strcmp(mapstr, "random")){
        static int times_called = 0;
        int i;
        times_called++;
        assert(proc);
        for ( i=1; i<size-1; i++) if ( proc[i] == rank) { prevproc = proc[i-1]; nextproc = proc[i+1]; }
        if  ( proc[size-1] == rank ) { prevproc = proc[size-2]; nextproc = proc[0];}
        if  ( proc[0] == rank ) { prevproc = proc[size-1]; nextproc = proc[1];}
        if (times_called == size){
            free(proc);
        }
    }
    else {
        prevproc = (0==rank) ? size-1 : rank-1; 
        nextproc = (size-1==rank) ? 0 : rank+1;
    }

    *prevproc_ptr = prevproc;
    *nextproc_ptr = nextproc;
    return (0);
}
