using System.Diagnostics;
using System.Collections.Concurrent;

static class ParallelFold
{
    static int N = 1_000_000_000;
    static int[] a = null!;
    static readonly int Cores = Environment.ProcessorCount;

    record Result(string Name, int Threads, string Chunk, double Ms, bool Ok);

    static void Fill()
    {
        const int B = 1 << 20;
        Parallel.For(0, (N + B - 1) / B, b =>
        {
            int s = b * B, e = Math.Min(s + B, N);
            for (int i = s; i < e; i++) a[i] = (i & 1) + 1;
        });
    }

    static long Expected() => (long)N + N / 2;

    static int Sequential()
    {
        var arr = a;
        int len = N;
        while (len > 1)
        {
            int half = len >> 1;
            for (int i = 0; i < half; i++) arr[i] += arr[len - 1 - i];
            len -= half;
        }
        return arr[0];
    }

    static int ManagerWorkers(int P, int chunk)
    {
        var start = new Barrier(P + 1);
        var end = new Barrier(P + 1);
        int next = 0, curLen = 0, curHalf = 0;
        bool done = false;

        void Worker()
        {
            var arr = a;
            while (true)
            {
                start.SignalAndWait();
                if (Volatile.Read(ref done)) return;
                int len = curLen, half = curHalf, s;
                while ((s = Interlocked.Add(ref next, chunk) - chunk) < half)
                {
                    int e = Math.Min(s + chunk, half);
                    for (int i = s; i < e; i++) arr[i] += arr[len - 1 - i];
                }
                end.SignalAndWait();
            }
        }

        var ts = new Thread[P];
        for (int t = 0; t < P; t++) { ts[t] = new Thread(Worker) { IsBackground = true }; ts[t].Start(); }

        int L = N;
        while (L > 1)
        {
            int half = L >> 1;
            curLen = L; curHalf = half; Volatile.Write(ref next, 0);
            start.SignalAndWait();
            end.SignalAndWait();
            L -= half;
        }
        Volatile.Write(ref done, true);
        start.SignalAndWait();
        foreach (var t in ts) t.Join();
        start.Dispose(); end.Dispose();
        return a[0];
    }

    static int ParallelForRanges(int P, int chunk)
    {
        var opt = new ParallelOptions { MaxDegreeOfParallelism = P };
        int len = N;
        while (len > 1)
        {
            int half = len >> 1, L = len;
            Parallel.ForEach(Partitioner.Create(0, half, chunk), opt, r =>
            {
                var arr = a;
                for (int i = r.Item1; i < r.Item2; i++) arr[i] += arr[L - 1 - i];
            });
            len -= half;
        }
        return a[0];
    }

    static int TaskSlices(int P)
    {
        int len = N;
        while (len > 1)
        {
            int half = len >> 1, L = len;
            int slices = Math.Min(half, P * 4);
            int step = (half + slices - 1) / slices;
            var tasks = new List<Task>(slices);
            for (int s = 0; s < half; s += step)
            {
                int from = s, to = Math.Min(s + step, half);
                tasks.Add(Task.Run(() => { var arr = a; for (int i = from; i < to; i++) arr[i] += arr[L - 1 - i]; }));
            }
            Task.WaitAll(tasks.ToArray());
            len -= half;
        }
        return a[0];
    }

    static double Bench(string name, int threads, string chunk, Func<int> algo, int reps, List<Result> results)
    {
        double best = double.MaxValue; bool ok = true;
        for (int r = 0; r < reps; r++)
        {
            Fill();
            var sw = Stopwatch.StartNew();
            long sum = (uint)algo();
            sw.Stop();
            ok &= sum == Expected();
            best = Math.Min(best, sw.Elapsed.TotalMilliseconds);
        }
        results.Add(new Result(name, threads, chunk, best, ok));
        Console.WriteLine($"  .. {name,-26} thr={threads,-3} chunk={chunk,-9} {best,9:F1} мс {(ok ? "" : "ПОМИЛКА")}");
        return best;
    }

    static void Main(string[] args)
    {
        if (args.Length > 0) N = int.Parse(args[0]);
        int reps = args.Length > 1 ? int.Parse(args[1]) : 2;
        Console.WriteLine($".NET {Environment.Version} | N={N:N0} | ядер: {Cores} | повторів: {reps} (береться найкращий)");
        a = new int[N];

        var res = new List<Result>();
        double seq = Bench("Послідовний", 1, "-", Sequential, reps, res);

        var threadSet = new SortedSet<int> { 1, 2, 4, Math.Max(1, Cores / 2), Cores, Cores * 2 };
        int[] chunks = { 1 << 10, 1 << 14, 1 << 18, 1 << 22 };
        foreach (int p in threadSet)
            foreach (int c in chunks)
                Bench("Керуючий+робочі", p, c.ToString(), () => ManagerWorkers(p, c), reps, res);

        Bench("Parallel.ForEach+Partitioner", Cores, (1 << 18).ToString(), () => ParallelForRanges(Cores, 1 << 18), reps, res);
        Bench("Task (статичні зрізи)", Cores, "P*4 зрізи", () => TaskSlices(Cores), reps, res);

        Console.WriteLine("\n================ ЗВІТ (C#) ================");
        Console.WriteLine($"{"Підхід",-30} {"Потоки",6} {"Порція",10} {"Час, мс",11} {"Приск.",8} OK");
        foreach (var r in res.OrderBy(r => r.Ms).Take(12))
            Console.WriteLine($"{r.Name,-30} {r.Threads,6} {r.Chunk,10} {r.Ms,11:F1} {seq / r.Ms,7:F2}x {(r.Ok ? "✓" : "✗")}");
        var b = res.OrderBy(r => r.Ms).First();
        Console.WriteLine($"\nНайкраща конфігурація: {b.Name}, потоків={b.Threads}, порція={b.Chunk} -> {b.Ms:F1} мс ({seq / b.Ms:F2}x до послідовного)");
        Console.WriteLine($"Сума = {(uint)a[0]:N0} (очікувана {Expected():N0})");
    }
}
