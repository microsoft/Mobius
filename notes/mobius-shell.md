# Mobius Shell

## Prerequisites

* All prerequisites listed in [Build in Windows](./windows-instructions.md#prerequisites)
* .NET Framework **4.6.1** or above.

**To Be Noticed**:
Currently Mobius Shell only supports **Windows**, Linux support is not supported yet.

## Building

Mobius Shell is included in Mobius release, please refer to [Build in Windows](./windows-instructions.md) to build Mobius.

## Running the Shell

Please first refer to [Running Mobius App](./running-mobius-app.md) to make sure all required softwares and enviroment variables are placed or set properly.

* Local

`scripts\sparkclr-repl.cmd --conf spark.local.dir=D:\tmp\repl`

* Yarn

`scripts\sparkclr-repl.cmd  --master yarn-client --num-executors 2 --executor-cores 2 --executor-memory 2G`

For more information about supported options with different deploy modes, please refer to `sparkclr-shell.cmd --help`

## Built-in directives in Shell

|Directive |Description|
|:---------|:----------|
|`:help`|Display help on available commands|
|`:load`|Load extra **local** library to current execution context, e.g. `:load "myLib.dll"`|
|`:quit`|Exit Mobius Shell  |


## Example
```
scripts\sparkclr-shell.cmd
Spark context available as sc.
SQL context available as sqlContext.
Use :quit to exit.
Type ":help" for more information.
>
> :help
Commands:
  :help         Display help on available commands.
  :load         Load extra library to current execution context, e.g. :load "myLib.dll".
  :quit         Exit REPL.
>
> // example of loading extra library
> :load "d:\test\TestLib.dll"
Loaded assebmly from d:\test\TestLib.dll
> using TestLib;
> new Lib().Test()
This is from test lib.
>
> // RDD example
> using System.Linq;
> var rdd = sc.Parallelize(Enumerable.Range(0, 100), 2).Map(d => d+1);
> rdd.Count()
100
>
> // dataframe example
> var df = sqlContext.Read().Json(@"D:\SparkWorkspace\SparkCLR_REPL\build\runtime\data\people.json");
> df.Count()
4
> df.Show()
+--------------------+---+---+-----+
|             address|age| id| name|
+--------------------+---+---+-----+
|     [Columbus,Ohio]| 34|123| Bill|
|   [null,California]| 44|456|Steve|
|[Seattle,Washington]| 43|789| Aaron|
+--------------------+---+---+-----+
> :quit
```
