using Serialize.Linq.Nodes;
using Serialize.Linq.Serializers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ExpressionSerializer.ComplexSerializer
{
    public class JsonLinqSerializer : JsonSerializer
    {
        public JsonLinqSerializer() : base()
        {
            this.AutoAddKnownTypesAsListTypes = true;
        }      

        public override void Serialize<T>(Stream stream, T obj)
        {
            Newtonsoft.Json.JsonSerializer serializer = Newtonsoft.Json.JsonSerializer.Create(
                new Newtonsoft.Json.JsonSerializerSettings
                {
                    TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Objects,
                    TypeNameAssemblyFormatHandling = Newtonsoft.Json.TypeNameAssemblyFormatHandling.Full,
                    ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore
                });
            using (StreamWriter sw = new StreamWriter(stream, Encoding.UTF8, 1024, true))
            using (Newtonsoft.Json.JsonWriter writer = new Newtonsoft.Json.JsonTextWriter(sw))
            {
                //var expression = typeNodeObj.ToString();
                if (obj is ExpressionNode typeNodeObj)
                {
                    serializer.Serialize(writer, obj, typeNodeObj.GetType());
                }
                else
                {
                    if (obj != null) serializer.Serialize(writer, obj);
                }
            }
        }

        public override T Deserialize<T>(Stream stream)
        {
            Newtonsoft.Json.JsonSerializer serializer = Newtonsoft.Json.JsonSerializer.Create(
                new Newtonsoft.Json.JsonSerializerSettings
                {
                    TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Objects,
                    TypeNameAssemblyFormatHandling = Newtonsoft.Json.TypeNameAssemblyFormatHandling.Full
                });
            using (StreamReader sr = new StreamReader(stream))
            using (Newtonsoft.Json.JsonReader reader = new Newtonsoft.Json.JsonTextReader(sr))
            {
                var expressionNode = serializer.Deserialize(reader) as T;
                return expressionNode;
            }
        }
    }
}
