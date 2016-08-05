# Real Flow - Code of Each Phase

### Work Flow

#### Phase 1 - Merge Log
* MergeLog.java
  * GigaPop Router Netflow + NetFlow* ( pcap -> NetFlow )
```sh
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.MergeLog
-D pcapInitialTime=<time 1> -D netflowTime=<time 2>
<NetFlow file> <NetFlow* file> <output file>
# time 1 : start time of <NetFlow* file> , format : yyyy-MM-dd_HH:mm:ss.SSS
# time 2 : start time of <NetFlow file> , format : unix time (millisecond)
# NetFlow 時間為NetFlow檔案第一行 第一欄位*1000 + 第二欄位 的數值

ex :
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.MergeLog
-D pcapInitialTime=2015-09-03_13:37:33.481 -D netflowTime=1428076802264
ncku0404_13 emptyfile merge_out_0413
```

#### Phase 2 - Filter 1
* FilterPhase1MR.java
  * 1. filter out white list and dns list
  * 2. group flow into session by TCP/UDP timeout
  * 3. extract features of session
```sh
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.FilterPhase1MR
-D filterdomain=<true/false> -D tcptime=<time 1> -D udptime=<time 2> -D mapreduce.job.reduces=15
<Phase 1 output> <output file>
# filterdomain=true : only retain IP with specified domain (ex: 140.116)
# time 1 : tcp timeout
# time 2 : udp timeout

ex :
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.FilterPhase1MR
-D filterdomain=false -D tcptime=21000 -D udptime=22000 -D mapreduce.job.reduces=15
merge_out_0413 filter1_out_0413
```

#### Phase 3 - Filter 2
* FilterPhase2MR.java
  * filter out flows with low flow loss rate
```sh
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.FilterPhase2MR
-D flowlossratio=<value> -D mapreduce.job.reduces=15 
<Phase 2 output> <output file>
# value : flow loss rate threshold

ex :
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.FilterPhase2MR
-D flowlossratio=0.225 -D mapreduce.job.reduces=15 
filter1_out_0413 filter2_out_0413
```

#### Phase 4 - Group 1
* GroupPhase1MR.java
  * group flows with the same source IP and destination IP by similarity formula
```sh
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.GroupPhase1MR
-D similarity=<value 1> -D repeatedlyConnectThreshold=<value 2> -D mapreduce.job.reduces=15
<Phase 3 output>/BiDirectional <output file>
# value 1 : similarity value
# value 2 : the least number of flow to be grouped

ex :
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.GroupPhase1MR
-D similarity=0.9 -D repeatedlyConnectThreshold=3 -D mapreduce.job.reduces=15
filter2_out_0413/BiDirectional group1_out_0413
```

#### Phase 5 - Group 2
* GroupPhase2MR.java
  * 1. group flows with the same source IP and different destination IP by similarity formula
  * 2. group flows with different source IP and different destination IP by DBSCAN
```sh
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.GroupPhase2MR
-D similarity=<value 1> -D similarityBH=<value 2> -D similarBehaviorThreshold=<value 3> -D mapreduce.job.reduces=15
<Phase 4 output> <output file 1> <output file 2>
# value 1 : similarity value for 1.
# value 2 : similarity value for 2.
# value 3 : the least number of flow to be grouped

ex :
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.GroupPhase2MR
-D similarity=0.9 -D similarityBH=0.9 -D similarBehaviorThreshold=3 -D mapreduce.job.reduces=15
group1_out_0413 group2_out_0413 fvidipmapping_0413
```

#### Phase 6 - Get the detected IP
* GetNegativeIPs.java
  * get the result
```sh
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.GetNegativeIPs
<Phase 3 output>/BiDirectional <Phase 3 output>/UniDirectional <Phase 5 output> <output file>
# using Phase 3 & 5 output as input

ex :
bin/hadoop jar project-0.0.1-SNAPSHOT.jar fbicloud.algorithm.botnetDetectionFS.GetNegativeIPs
filter2_out_0413/BiDirectional filter2_out_0413/UniDirectional group2_out_0413 getip_out_0413
```
