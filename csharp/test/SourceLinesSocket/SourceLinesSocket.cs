using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SourceLinesSocket
{
    class SourceLinesSocket
    {
        private static Socket serverSocket;
        private static IPAddress hostAddress;

        private static volatile bool IsNeedStop = false;

        const string TimeFormat = "yyyy-MM-dd HH:mm:ss";
        const string MilliTimeFormat = TimeFormat + ".fff";
        const string MicroTimeFormat = MilliTimeFormat + "fff";

        static void Log(string message)
        {
            Console.WriteLine("{0} SourceLinesSocket : {1}", DateTime.Now.ToString(MilliTimeFormat), message);
        }

        static void Main(string[] args)
        {
            var exe = System.Reflection.Assembly.GetExecutingAssembly().CodeBase;
            var exeName = Path.GetFileName(exe);

            if (args.Length < 1 || args[0] == "-h" || args[0] == "--help")
            {
                Console.WriteLine("Usage :    {0}  port  [send-interval-milliseconds]   [running-seconds]  [bind-host]", exeName);
                Console.WriteLine("Example-1: {0}  9111  100                            3600               127.0.0.1", exeName);
                Console.WriteLine("Example-2: {0}  9111  100  3600 {1}", exeName, GetHost(false));
                return;
            }

            int idxArg = -1;
            Func<string, string, bool, string> getArg = (name, defaultArgValue, canBeNotSet) =>
            {
                idxArg++;
                if (args.Length > idxArg)
                {
                    Log(string.Format("args[{0}] : {1} = {2}", idxArg, name, args[idxArg]));
                    return args[idxArg];
                }
                else if (canBeNotSet)
                {
                    Log(string.Format("args--{0} : {1} = {2}", idxArg, name, defaultArgValue));
                    return defaultArgValue;
                }
                else
                {
                    throw new ArgumentException(string.Format("cannot omit {0} at arg[{0}]", name, idxArg + 1), name);
                }
            };

            var port = int.Parse(getArg("port", "", false));
            var milliInterval = int.Parse(getArg("milliseconds-send-interval", "1000", true));
            var runningSeconds = int.Parse(getArg("running-seconds", "3600", true));
            var hostToUse = getArg("host-to-force-use", "", true);
            var duration = runningSeconds <= 0 ? TimeSpan.MaxValue : new TimeSpan(0, 0, 0, runningSeconds);

            hostAddress = !string.IsNullOrWhiteSpace(hostToUse) ? IPAddress.Parse(hostToUse) : GetHost();
            serverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            serverSocket.Bind(new IPEndPoint(hostAddress, port));
            serverSocket.Listen(10);

            var startTime = DateTime.Now;
            var thread = new Thread(() => { ListenToClient(startTime, milliInterval, duration); });
            thread.IsBackground = runningSeconds > 0;
            thread.Start();

            if (runningSeconds < 1)
            {
                return;
            }

            var process = Process.GetCurrentProcess();
            var stopTime = startTime + duration;

            Log("will stop at " + stopTime.ToString(MilliTimeFormat));
            Thread.Sleep(runningSeconds * 1000);
            Log("passed " + (DateTime.Now - startTime).TotalSeconds + " s, thread id = " + thread.ManagedThreadId + " , state = " + thread.ThreadState + ", isAlive = " + thread.IsAlive);
            while (DateTime.Now < stopTime)
            {
                var remainSleep = (stopTime - DateTime.Now).TotalSeconds;
                Log("passed " + (DateTime.Now - startTime).TotalSeconds + " s, still need wait " + remainSleep + " s. Thread id = " + thread.ManagedThreadId + " , state = " + thread.ThreadState + ", isAlive = " + thread.IsAlive);
                Thread.Sleep((int)(remainSleep * 1000));
            }

            IsNeedStop = true;
            Thread.Sleep(Math.Max(100, milliInterval));
            Log("passed " + (DateTime.Now - startTime).TotalSeconds + " s, thread id = " + thread.ManagedThreadId + " , state = " + thread.ThreadState + ", isAlive = " + thread.IsAlive);
            if (thread.IsAlive)
            {
                Log("try to kill process " + process.Id + " to stop thread id = " + thread.ManagedThreadId + " , state = " + thread.ThreadState);
                process.Kill();
                Log("try to exit to stop thread " + thread + " , state = " + thread.ThreadState);
                Environment.Exit(0);
            }

            serverSocket.Dispose();
        }

        private static IPAddress GetHost(bool print = false)
        {
            IPAddress[] ips = Dns.GetHostAddresses(Dns.GetHostName());
            foreach (IPAddress ipa in ips)
            {
                if (ipa.AddressFamily != AddressFamily.InterNetwork)
                {
                    continue;
                }

                if (print)
                {
                    Console.WriteLine("ip = {0}, AddressFamily = {1}", ipa, ipa.AddressFamily);
                }

                var ip = ipa.ToString();
                if (ip.StartsWith("10.0.2.") && !ip.StartsWith("192.168."))
                {
                    return ipa;
                }
            }
            
            return IPAddress.Parse("127.0.0.1");
        }
        
        private static void ListenToClient(DateTime startTime, int milliInterval, TimeSpan runningDuration, int times = 1)
        {
            var stopTime = runningDuration == TimeSpan.MaxValue ? "endless" : (startTime + runningDuration).ToString(MilliTimeFormat);
            if (DateTime.Now - startTime > runningDuration)
            {
                Log("Not running. start from " + startTime.ToString(MilliTimeFormat) + " , running for " + runningDuration);
                return;
            }
            else
            {
                Log("Machine = " + Environment.MachineName + ", OS = " + Environment.OSVersion
                    + ", start listening " + serverSocket.LocalEndPoint.ToString()
                    + (runningDuration == TimeSpan.MaxValue ? "  running endless " : "  will stop at " + stopTime));
            }

            var count = 0;
            try
            {
                Socket clientSocket = serverSocket.Accept();
                while (!IsNeedStop)
                {
                    if (DateTime.Now - startTime > runningDuration)
                    {
                        Log("Stop running. start from " + startTime.ToString(MilliTimeFormat) + " , running for " + runningDuration);
                        break;
                    }
                    count++;
                    var message = string.Format("{0} from '{1}' '{2}' {3} times[{4}] send message[{5}] to {6} : tick = {7}{8}",
                        DateTime.Now.ToString(MicroTimeFormat), Environment.OSVersion, Environment.MachineName, hostAddress, times, count, clientSocket.RemoteEndPoint, DateTime.Now.Ticks, Environment.NewLine);
                    Console.Write(message);
                    clientSocket.Send(Encoding.ASCII.GetBytes(message));
                    Thread.Sleep(milliInterval);
                }

                clientSocket.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                var thread = new Thread(() => { ListenToClient(startTime, milliInterval, runningDuration, times + 1); });
                thread.Start();
            }
        }
    }
}
