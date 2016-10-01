#!/bin/sh
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar wordcount -Dfed=true -DfedHdfs=true -DwanOpt=false -Dmain=wordcount -DtopNumbers=1 -DArg0=test_input2 -DArg1=fed_output_reallocate test_input2 fed_output_reallocate 

