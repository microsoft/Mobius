# Implementing Spark Apps in F# using Mobius

## Non-Interactive Apps
1. Develop your application in a F# IDE using Mobius API. Refer to [F# examples](../examples/fsharp) for sample code
2. Use [`sparkclr-submit.cmd`](running-mobius-app.md) to run your Mobius-based Spark application implemented in F#

## Interactive Apps
### Using F# Interactive (fsi.exe)
1. Run `sparkclr-submit.cmd debug` in a command prompt after setting necessary [environment variables](running-mobius-app.md#pre-requisites). Note that this `debug` parameter is a misnomer in this context and this command initializes .NET-JVM bridge similiar to [running Mobius apps in debug mode](./running-mobius-app.md#debug-mode).
2. In Developer Command Prompt for VS, run `fsi.exe --use:c:\temp\mobius-init.fsx`. [mobius-init.fsx](mobius-init.fsx) has the initialization code that can be used to create `SparkContext`, `SqlContext` or `SparkSession`. You need to update the location of Mobius binaries referenced in the beginning of the script file. You may also need to update other configuration settings in the script.
3. When the F# command prompt is available, Spark functionality can be invoked using Mobius API. For example, the following code can be used process JSON file.
```
let dataframe = sparkSession.Read().Json @"C:\temp\data.json";;
dataframe.Show();;
dataframe.ShowSchema();;
dataframe.Count();;
```
