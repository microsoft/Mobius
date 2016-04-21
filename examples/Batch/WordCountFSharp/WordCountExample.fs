open Microsoft.Spark.CSharp.Core
open Microsoft.Spark.CSharp.Services
open System.Reflection
open System.Collections.Generic

LoggerServiceFactory.SetLoggerService Log4NetLoggerService.Instance
let logger = LoggerServiceFactory.GetLogger (MethodInfo.GetCurrentMethod().DeclaringType)

[<EntryPoint>]
let main = function
    | [| filePath |] ->
        let sparkContext = SparkConf().SetAppName "MobiusWordCount" |> SparkContext
        logger.LogInfo (sprintf "Reading from file %s" filePath)

        try
            let lines = sparkContext.TextFile filePath
            let counts =
                lines.FlatMap(fun x -> x.Split ' ' :> _)
                     .Map(fun w -> KeyValuePair(w, 1))
                     .ReduceByKey(fun x y -> x + y)
                     .Collect()
            for count in counts do
                printfn "%s: %d" count.Key count.Value
        with
        | ex ->
            logger.LogError "Error performing Word Count"
            logger.LogException ex

        sparkContext.Stop()
        0
    | _ ->        
        printfn "Usage: WordCount <file>"
        1
