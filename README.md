#PapJuice

*MapReduce through Python Goggles*

## Inspiration
To learn & practice concepts of MapReduce (based on Hadoop) & MultiProcessing.

## Plan
We will approach the problem in two phases:
* Create a Single Process solution.
* Convert it to Multi Process.

Step 1 ensures that we have correct solution that we can verify against Step 2.

## Concept of MapReduce via cliché - Word Count
Consider we have three documents and we want to find the collective count of unique words in them. We can use MapReduce to take advantage of distributed computing.

If we were to use a production level MapReduce solution we would be using Hadoop or Compute, but since our attempt is to learn MultiProcessing, we will be creating our own MapReduce solution. This way, we also learn about the various components of MapReduce.

MapReduce consists of five major components (in most simplistic terms):
* Input Streams
* Mapper
* Sorter
* Reducer
* Output Stream

## Understanding MapReduce Components 

### Input Streams
For initial development purpose, we will use a list of three String Arrays of known word count. 
From application standpoint, it just expects an iterator of number of input streams, where each input stream is an iterator.

### Mapper
Our Mapper is a function that takes a given data stream and creates mapped data that will be used by Reducer to count words. The mapped data will be a list of tuples (\<word>, 1) where each \<word> can be repeated multiple times in the list.

### Sorter
Given that we have three input streams, each input stream will have a list of tuples and each word can be repeated multiple times. We want to ensure that same word is sent to a single Reducer and it is Sorter's responsibility to ensure this happens. 

Sorter achieves this by clubbing together tuple values (\<word>, *value =* 1) for each distinct word. A HashMap is a good way to achieve this.

### Reducer
Reducer takes each Key-Value pair from Sorter's HashMap and sums up the Value for each Key. Sends back another HashMap and this forms our Output.

### Output Stream
We send our Reducer output to Output Stream. We use ```print```, but it can easily be substituted with a different function.