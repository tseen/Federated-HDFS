echo "make workspace file" 
ssh b17 "mkdir ~/ton/hadoop/hadoop-2.2.0-src/hadoop-dist/target/hadoop-2.2.0/workspace"

echo "make name file"
ssh b17 "mkdir ~/ton/hadoop/hadoop-2.2.0-src/hadoop-dist/target/hadoop-2.2.0/workspace/name"

echo "make data file"
ssh b17 "mkdir ~/ton/hadoop/hadoop-2.2.0-src/hadoop-dist/target/hadoop-2.2.0/workspace/data"

echo "make tmp file"
ssh b17 "mkdir ~/ton/hadoop/hadoop-2.2.0-src/hadoop-dist/target/hadoop-2.2.0/workspace/tmp"

echo "cp conf"
ssh b17 "cp -r ~/ton/hadoop/hadoop-2.2.0-src/hadoop ~/ton/hadoop/hadoop-2.2.0-src/hadoop-dist/target/hadoop-2.2.0/etc"

echo "cp memAPI"
ssh b17 "cp ~/ton/spymemcached-2.9.1.jar ~/ton/hadoop/hadoop-2.2.0-src/hadoop-dist/target/hadoop-2.2.0/share/hadoop/common/lib/"
