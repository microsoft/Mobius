using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Sql;

namespace Tests.Common
{
    internal class RowHelper
    {
        public const string BasicJsonSchema = @"
                {
                  ""type"" : ""struct"",
                  ""fields"" : [{
                    ""name"" : ""age"",
                    ""type"" : ""long"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""id"",
                    ""type"" : ""string"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""name"",
                    ""type"" : ""string"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  } ]
                }";

        public const string ComplexJsonSchema = @"
                {
                  ""type"" : ""struct"",
                  ""fields"" : [ {
                    ""name"" : ""address"",
                    ""type"" : {
                      ""type"" : ""struct"",
                      ""fields"" : [ {
                        ""name"" : ""city"",
                        ""type"" : ""string"",
                        ""nullable"" : true,
                        ""metadata"" : { }
                      }, {
                        ""name"" : ""state"",
                        ""type"" : ""string"",
                        ""nullable"" : true,
                        ""metadata"" : { }
                      } ]
                    },
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""age"",
                    ""type"" : ""long"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""id"",
                    ""type"" : ""string"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""name"",
                    ""type"" : ""string"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""phone numbers"",
                    ""type"" : {
                        ""type"" : ""array"",
                        ""elementType"" : ""string"",
                        ""containsNull"" : true },
                    ""nullable"" : true,
                    ""metadata"" : { }
                  } ]
                }";

        public static readonly StructType BasicSchema;
        public static readonly StructType ComplexSchema;

        static RowHelper()
        {
            BasicSchema = DataType.ParseDataTypeFromJson(BasicJsonSchema) as StructType;
            ComplexSchema = DataType.ParseDataTypeFromJson(ComplexJsonSchema) as StructType;
        }

        public static Row BuildRowForBasicSchema(int seqId)
        {
            return new RowImpl(new object[] { seqId, "id " + seqId, "name" + seqId }, BasicSchema);
        }
    }
}
