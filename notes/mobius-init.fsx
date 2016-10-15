// *** Replace the paths below to point to correct location of Mobius binaries ***
#r @"C:\spark-clr_2.11-2.0.000\runtime\bin\Microsoft.Spark.CSharp.Adapter.dll"
#r @"C:\spark-clr_2.11-2.0.000\runtime\bin\log4net.dll"
#r @"C:\spark-clr_2.11-2.0.000\runtime\bin\Newtonsoft.Json.dll"
#r @"C:\spark-clr_2.11-2.0.000\runtime\bin\Razorvine.Pyrolite.dll"
#r @"C:\spark-clr_2.11-2.0.000\runtime\bin\Razorvine.Serpent.dll"
#r @"C:\spark-clr_2.11-2.0.000\runtime\bin\CSharpWorker.exe"
open Microsoft.Spark.CSharp.Core
open Microsoft.Spark.CSharp.Services
open Microsoft.Spark.CSharp.Sql
open System.Reflection
open System.Collections.Generic
LoggerServiceFactory.SetLoggerService Log4NetLoggerService.Instance

// *** Uncomment & use the following code block to use SqlContext API ***
//let conf = SparkConf().SetAppName "FSharpInteractiveShell"
// *** uncomment & update master URL if running in non-local mode ***
//conf.Master "spark://sparkmaster:7077"
// *** Spark 2.0 in Windows requires the following config ***
//conf.Set("spark.sql.warehouse.dir", @"file:///C:/sparktemp")
//let sc = SparkContext conf
//let sqlContext = SqlContext sc

// *** Uncomment & use the following code block to use SparkSession API ***
let builder = SparkSession.Builder()
builder = builder.AppName "FSharpInteractiveShell"
// *** uncomment & update master URL if running in non-local mode ***
//builder = builder.Master "spark://sparkmaster:7077"
// *** Spark 2.0 in Windows requires the following config ***
builder = builder.Config("spark.sql.warehouse.dir", "file:///C:/sparktemp")
let session = builder.GetOrCreate()
