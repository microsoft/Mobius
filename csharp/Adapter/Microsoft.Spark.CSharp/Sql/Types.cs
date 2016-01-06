// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Spark.CSharp.Sql
{
    [Serializable]
    public abstract class DataType
    {
        /// <summary>
        /// Trim "Type" in the end from class name, ToLower() to align with Scala.
        /// </summary>
        public string TypeName
        {
            get { return NormalizeTypeName(GetType().Name); }
        }

        /// <summary>
        /// return TypeName by default, subclass can override it
        /// </summary>
        public virtual string SimpleString
        {
            get { return TypeName; }
        }

        /// <summary>
        /// return only type: TypeName by default, subclass can override it
        /// </summary>
        internal virtual object JsonValue { get { return TypeName; } }

        public string Json
        {
            get
            {
                var jObject = JsonValue is JObject ? ((JObject)JsonValue).SortProperties() : JsonValue;
                return JsonConvert.SerializeObject(jObject, Formatting.None);
            }
        }

        public static DataType ParseDataTypeFromJson(string json)
        {
            return ParseDataTypeFromJson(JToken.Parse(json));
        }

        protected static DataType ParseDataTypeFromJson(JToken json)
        {
            if (json.Type == JTokenType.Object) // {name: address, type: {type: struct,...},...}
            {
                JToken type;
                var typeJObject = (JObject)json;
                if (typeJObject.TryGetValue("type", out type))
                {
                    Type complexType;
                    if ((complexType = ComplexTypes.FirstOrDefault(ct => NormalizeTypeName(ct.Name) == type.ToString())) != default(Type))
                    {
                        return ((ComplexType)Activator.CreateInstance(complexType, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
                            , null, new object[] { typeJObject }, null)); // create new instance of ComplexType
                    }
                    if (type.ToString() == "udt")
                    {
                        // TODO
                        throw new NotImplementedException();
                    }
                }
                throw new ArgumentException(string.Format("Could not parse data type: {0}", type));
            }
            else // {name: age, type: bigint,...} // TODO: validate more JTokenType other than Object
            {
                return ParseAtomicType(json);
            }

        }

        private static AtomicType ParseAtomicType(JToken type)
        {
            Type atomicType;
            if ((atomicType = AtomicTypes.FirstOrDefault(at => NormalizeTypeName(at.Name) == type.ToString())) != default(Type))
            {
                return (AtomicType)Activator.CreateInstance(atomicType); // create new instance of AtomicType
            }

            Match fixedDecimal = DecimalType.FixedDecimal.Match(type.ToString());
            if (fixedDecimal.Success)
            {
                return new DecimalType(int.Parse(fixedDecimal.Groups[1].Value), int.Parse(fixedDecimal.Groups[2].Value));
            }

            throw new ArgumentException(string.Format("Could not parse data type: {0}", type));
        }

        [NonSerialized]
        private static readonly Type[] AtomicTypes = typeof(AtomicType).Assembly.GetTypes().Where(type =>
            type.IsSubclassOf(typeof(AtomicType))).ToArray();

        [NonSerialized]
        private static readonly Type[] ComplexTypes = typeof(ComplexType).Assembly.GetTypes().Where(type =>
            type.IsSubclassOf(typeof(ComplexType))).ToArray();

        [NonSerialized]
        private static readonly Func<string, string> NormalizeTypeName = s => s.Substring(0, s.Length - 4).ToLower(); // trim "Type" at the end of type name


    }

    [Serializable]
    public class AtomicType : DataType
    {
    }

    [Serializable]
    public abstract class ComplexType : DataType
    {
        public abstract DataType FromJson(JObject json);
        public DataType FromJson(string json)
        {
            return FromJson(JObject.Parse(json));
        }
    }


    [Serializable]
    public class NullType : AtomicType { }

    [Serializable]
    public class StringType : AtomicType { }

    [Serializable]
    public class BinaryType : AtomicType { }

    [Serializable]
    public class BooleanType : AtomicType { }

    [Serializable]
    public class DateType : AtomicType { }

    [Serializable]
    public class TimestampType : AtomicType { }

    [Serializable]
    public class DoubleType : AtomicType { }

    [Serializable]
    public class FloatType : AtomicType { }

    [Serializable]
    public class ByteType : AtomicType { }

    [Serializable]
    public class IntegerType : AtomicType { }

    [Serializable]
    public class LongType : AtomicType { }

    [Serializable]
    public class ShortType : AtomicType { }

    [Serializable]
    public class DecimalType : AtomicType
    {
        public static Regex FixedDecimal = new Regex(@"decimal\((\d+),\s(\d+)\)");
        private int? precision, scale;
        public DecimalType(int? precision = null, int? scale = null)
        {
            this.precision = precision;
            this.scale = scale;
        }

        internal override object JsonValue
        {
            get { throw new NotImplementedException(); }
        }

        public DataType FromJson(JObject json)
        {
            throw new NotImplementedException();
        }
    }

    [Serializable]
    public class ArrayType : ComplexType
    {
        public DataType ElementType { get { return elementType; } }
        public bool ContainsNull { get { return containsNull; } }

        public ArrayType(DataType elementType, bool containsNull = true)
        {
            this.elementType = elementType;
            this.containsNull = containsNull;
        }

        internal ArrayType(JObject json)
        {
            FromJson(json);
        }

        public override string SimpleString
        {
            get { return string.Format("array<{0}>", elementType.SimpleString); }
        }

        internal override object JsonValue
        {
            get
            {
                return new JObject(
                                  new JProperty("type", TypeName),
                                  new JProperty("elementType", elementType.JsonValue),
                                  new JProperty("containsNull", containsNull));
            }
        }

        public override sealed DataType FromJson(JObject json)
        {
            elementType = ParseDataTypeFromJson(json["elementType"]);
            containsNull = (bool)json["containsNull"];
            return this;
        }

        private DataType elementType;
        private bool containsNull;
    }

    [Serializable]
    public class MapType : ComplexType
    {
        internal override object JsonValue
        {
            get { throw new NotImplementedException(); }
        }

        public override DataType FromJson(JObject json)
        {
            throw new NotImplementedException();
        }
    }

    [Serializable]
    public class StructField : ComplexType
    {
        public string Name { get { return name; } }
        public DataType DataType { get { return dataType; } }
        public bool IsNullable { get { return isNullable; } }
        public JObject Metadata { get { return metadata; } }

        public StructField(string name, DataType dataType, bool isNullable = true, JObject metadata = null)
        {
            this.name = name;
            this.dataType = dataType;
            this.isNullable = isNullable;
            this.metadata = metadata ?? new JObject();
        }

        internal StructField(JObject json)
        {
            FromJson(json);
        }

        public override string SimpleString { get { return string.Format(@"{0}:{1}", name, dataType.SimpleString); } }

        internal override object JsonValue
        {
            get
            {
                return new JObject(
                            new JProperty("name", name),
                            new JProperty("type", dataType.JsonValue),
                            new JProperty("nullable", isNullable),
                            new JProperty("metadata", metadata));
            }
        }

        public override sealed DataType FromJson(JObject json)
        {
            name = json["name"].ToString();
            dataType = ParseDataTypeFromJson(json["type"]);
            isNullable = (bool)json["nullable"];
            metadata = (JObject)json["metadata"];
            return this;
        }

        private string name;
        private DataType dataType;
        private bool isNullable;
        [NonSerialized]
        private JObject metadata;
    }

    [Serializable]
    public class StructType : ComplexType
    {
        public List<StructField> Fields { get { return fields; } }

        internal IStructTypeProxy StructTypeProxy
        {
            get
            {
                return structTypeProxy ?? 
                    new StructTypeIpcProxy(
                        new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "createSchema",
                            new object[] { Json }).ToString()));
            }
        }

        public StructType(IEnumerable<StructField> fields)
        {
            this.fields = fields.ToList();
        }

        internal StructType(JObject json)
        {
            FromJson(json);
        }

        internal StructType(IStructTypeProxy structTypeProxy)
        {
            this.structTypeProxy = structTypeProxy;
            var jsonSchema = structTypeProxy.ToJson();
            FromJson(jsonSchema);
        }

        public override string SimpleString
        {
            get { return string.Format(@"struct<{0}>", string.Join(",", fields.Select(f => f.SimpleString))); }
        }

        internal override object JsonValue
        {
            get
            {
                return new JObject(
                                new JProperty("type", TypeName),
                                new JProperty("fields", fields.Select(f => f.JsonValue).ToArray()));
            }
        }

        public override sealed DataType FromJson(JObject json)
        {
            var fieldsJObjects = json["fields"].Select(f => (JObject)f);
            fields = fieldsJObjects.Select(fieldJObject => (new StructField(fieldJObject))).ToList();
            return this;
        }

        [NonSerialized]
        private readonly IStructTypeProxy structTypeProxy;

        private List<StructField> fields;
    }

}
