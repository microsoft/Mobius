#Using Mobius in HDInsight Spark Cluster
Mobius [requires](./linux-instructions.md#prerequisites) Mono version 4.2 or above. Depending on the HDI cluster version, manual upgrade of Mono in head and worker nodes may be required. Refer to the table below for Mono upgrade requirements.

|HDI Version |Mono Version |Mono Upgrade Required |
|---|:------|:----|
3.4 |3.4 |Yes |
3.5 |4.6.1 |No |

After ensuring that the correct version of Mono is available in the HDI cluster, [instructions](./linux-instructions.md#requirements) to run Mobius applications in HDI are similiar to that of any Ubuntu-based Spark cluster using YARN. Following steps illustrate how to run Mobius Pi example in HDI.

```
# login to head node
# create mobius folder under /home/username
mkdir mobius
cd mobius
# replace the url below with the correct version of Mobius
wget https://github.com/Microsoft/Mobius/releases/download/v2.0.000-PREVIEW-2/spark-clr_2.11-2.0.000-PREVIEW-2.zip
unzip spark-clr_2.11-2.0.000-PREVIEW-2.zip
export SPARKCLR_HOME=/home/username/mobius/runtime
cd runtime/scripts
chmod +x sparkclr-submit.sh
# make sure Mobius app has executable permissions
chmod +x ../../examples/Batch/pi/SparkClrPi.exe
# deploy mode can be client or cluster
./sparkclr-submit.sh --master yarn --deploy-mode client --exe SparkClrPi.exe /home/username/mobius/examples/Batch/pi
```
