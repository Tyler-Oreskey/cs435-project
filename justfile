# Print our list of just commands
default:
    @just -l

# Run `mvn package`
compile:
    cd IceCapMonitor && mvn package

# Create directories and upload input data to hdfs
upload input:
    hadoop fs -mkdir -p /IceCapMonitor/input
    hadoop fs -mkdir -p /IceCapMonitor/output
    hadoop fs -put -t 5 {{input}} /IceCapMonitor/input

# Delete MapReduce output
cleanup:
    hadoop fs -rm -r /IceCapMonitor/output || true

# Compile and run sample dataset locally
local_sample: compile cleanup
    hadoop jar IceCapMonitor/target/*.jar edu.csu.icecapmonitor.IceCapMonitorMapReduce /IceCapMonitor/input/Sample_Dataset /IceCapMonitor/output

# Compile and run full dataset locally
local_full: compile cleanup
    hadoop jar IceCapMonitor/target/*.jar edu.csu.icecapmonitor.IceCapMonitorMapReduce /IceCapMonitor/input/Full_Dataset /IceCapMonitor/output

# Compile and run sample dataset using yarn
yarn_sample: compile cleanup
    hadoop jar IceCapMonitor/target/*.jar edu.csu.icecapmonitor.IceCapMonitorMapReduce -D mapreduce.framework.name=yarn /IceCapMonitor/input/Sample_Dataset /IceCapMonitor/output

# Compile and run full dataset using yarn
yarn_full: compile cleanup
    hadoop jar IceCapMonitor/target/*.jar edu.csu.icecapmonitor.IceCapMonitorMapReduce -D mapreduce.framework.name=yarn /IceCapMonitor/input/Full_Dataset /IceCapMonitor/output
