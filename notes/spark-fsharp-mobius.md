# Implementing Spark Apps in F# using Mobius

## Non-Interactive Apps
1. Develop your application in a F# IDE using Mobius API. Refer to F# [examples](../examples/fsharp) for sample code
2. Use [`sparkclr-submit.cmd`](running-mobius-app.md) to run your Mobius-based Spark application

## Interactive Apps
### Using F# Interactive (fsi.exe)
1. Run `sparkclr-submit.cmd debug` in a command prompt after setting necessary [environment variables](running-mobius-app.md#pre-requisites) 
2. In Developer Command Prompt, run `fsi.exe --use:mobius-init.fsx`. [mobius-init.fsx](mobius-init.fsx) has the initialization code. You need to update the location of Mobius binaries referenced in the script. You may also need to update other configuration settings in the script.
3. When the F# command prompt is available, Spark functionality can be invoked using Mobius API. For example, the following code can be used process JSON file.
```
let dataframe = sparkSession.Read().Json(@"C:\temp\data.json");;
dataframe.Show();;
dataframe.ShowSchema();;
dataframe.Count();;
```

### Using Jupyter/IFSharp
* Instructions to be added
