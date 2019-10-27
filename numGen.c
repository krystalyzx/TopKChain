#include <stdio.h>
#include <stdlib.h>
#include <time.h> 
int cmpfunc (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}
int main(int argc, char* argv[]) {
    int count = atoi(argv[1]);
    printf("%d\n",count);
    int* nums = calloc(count, sizeof(int));
    int n;
    int i;
    FILE *fp,*fps;
    fp = fopen("numbers.txt","w+");
    fps = fopen("sorted.txt","w+");
    srand(time(NULL));  
    for (i = 0; i < count; i++) {
        n = rand();
        fprintf(fp, "%d\n", n);
        nums[i] = n;
    }
    fclose(fp);
    qsort(nums, count, sizeof(int), cmpfunc);
    for (i = 0;i < count; i++){
        fprintf(fps, "%d\n", nums[i]);
    }
    fclose(fps);
    free(nums);
    return 0;
}