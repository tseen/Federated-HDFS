#!/bin/sh
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar wordcount -Dfed=true -DfedHdfs=true -DfedIteration=3 -DtopNumbers=2 -Dmain=wordcount -DArg0=test_input2 -DArg1=fed_output12 test_input2 fed_output12
