# Top-k integer
Obtaining the top-k integer values in an input file using chain architecture.  Implemented in C using [FGMPI](https://www.cs.ubc.ca/~humaira/fgmpi.html).
## Compile
`make`

## Run
Run `mpiexec -nfg x -n y ./primeSieve numbers.txt z` to compute the top x*y*z integers in numbers.txt file using x*y processes.  Rank 1 process will have the smallest z intergers, Rank 2 processes will have the second smallest z integers, and so on.  
*Note*: In order for the program to run correctly, x*y*z must be greater than the number of integers
in the input file; otherwsie, the program will print an error message and abort

## Generate n random integers:
Run `./numGen n`  
numbers.txt contains the generated integers in the original order, one on each line  
sorted.txt contains the generated integers sorted in an incresing order, one on each line 
