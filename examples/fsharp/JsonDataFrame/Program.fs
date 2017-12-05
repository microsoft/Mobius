// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

open Microsoft.Spark.CSharp.Core
open Microsoft.Spark.CSharp.Services
open Microsoft.Spark.CSharp.Sql
open System.Reflection
open System.Collections.Generic

[<EntryPoint>]
let main args = 
    match args with
    | [| filePath |] ->
        
        let sparkContext = SparkContext(SparkConf().SetAppName "MobiusJsonDataFrame")
        let sqlContext = SqlContext sparkContext

        //reading dataframe
        let dataframe = sqlContext.Read().Json(filePath)
        dataframe.ShowSchema()
        dataframe.Show()

        //using DataFrame API
        let filteredDf = dataframe.Select("name", "address.state")
                                    .Where("state = 'California'")

        //using Spark SQL
        filteredDf.RegisterTempTable "temptable123"
        let countAsDf = sqlContext.Sql "SELECT * FROM temptable123 where name='Bill'"
        let countOfRows = countAsDf.Count()
        printfn "Count of rows with name='Bill' and State='California' = %d" countOfRows

        sparkContext.Stop()
        0
    | _ ->        
        printfn "Usage: JsonDataFrame <file>"
        1
