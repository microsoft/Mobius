// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Linq;
using Newtonsoft.Json.Linq;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Json.NET Serialization/Deserialization helper class.
    /// </summary>
    public static class JsonSerDe
    {
        // Note: Scala side uses JSortedObject when parse Json, so the properties in JObject need to be sorted
        /// <summary>
        /// Extend method to sort items in a JSON object by keys.
        /// </summary>
        /// <param name="jObject"></param>
        /// <returns></returns>
        public static JObject SortProperties(this JObject jObject)
        {
            JObject sortedJObject = new JObject();
            foreach (var property in jObject.Properties().OrderBy(p => p.Name))
            {
                if (property.Value is JObject)
                {
                    var propJObject = property.Value as JObject;
                    sortedJObject.Add(property.Name, propJObject.SortProperties());
                }
                else if (property.Value is JArray)
                {
                    var propJArray = property.Value as JArray;
                    sortedJObject.Add(property.Name, propJArray.SortProperties());
                }
                else
                {
                    sortedJObject.Add(property.Name, property.Value);
                }
            }
            return sortedJObject;
        }

        /// <summary>
        /// Extend method to sort items in a JSON array by keys.
        /// </summary>
        public static JArray SortProperties(this JArray jArray)
        {
            JArray sortedJArray = new JArray();
            if (jArray.Count == 0) return jArray;

            foreach (var item in jArray)
            {
                if (item is JObject)
                {
                    var sortedItem = ((JObject)item).SortProperties();
                    sortedJArray.Add(sortedItem);
                }
                else if (item is JArray)
                {
                    var sortedItem = ((JArray)item).SortProperties();
                    sortedJArray.Add(sortedItem);
                }
            }
            return sortedJArray;
        }
    }

}
