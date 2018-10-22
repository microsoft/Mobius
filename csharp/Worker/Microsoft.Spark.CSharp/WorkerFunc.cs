using System.Runtime.Serialization;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp
{
    internal class WorkerFunc
    {
        private int stageId;
        private CSharpWorkerFunc func;
        private int argsCount;

        public WorkerFunc(CSharpWorkerFunc func, int argsCount, int stageId)
        {
            this.func = func;
            this.argsCount = argsCount;
            this.stageId = stageId;
        }

        public int StageId
        {
            get
            {
                return stageId;
            }

            set
            {
                stageId = value;
            }
        }

        internal CSharpWorkerFunc Func
        {
            get
            {
                return func;
            }

            set
            {
                func = value;
            }
        }

        public int ArgsCount
        {
            get
            {
                return argsCount;
            }

            set
            {
                argsCount = value;
            }
        }
    }
}
