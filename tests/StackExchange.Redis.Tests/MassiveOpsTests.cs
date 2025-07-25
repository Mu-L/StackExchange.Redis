﻿using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace StackExchange.Redis.Tests;

[Collection(NonParallelCollection.Name)]
public class MassiveOpsTests(ITestOutputHelper output) : TestBase(output)
{
    [Fact]
    public async Task LongRunning()
    {
        Skip.UnlessLongRunning();
        await using var conn = Create();

        var key = Me();
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.StringSet(key, "test value", flags: CommandFlags.FireAndForget);
        for (var i = 0; i < 200; i++)
        {
            var val = await db.StringGetAsync(key).ForAwait();
            Assert.Equal("test value", val);
            await Task.Delay(50).ForAwait();
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MassiveBulkOpsAsync(bool withContinuation)
    {
        await using var conn = Create();

        RedisKey key = Me();
        var db = conn.GetDatabase();
        await db.PingAsync().ForAwait();
        static void NonTrivial(Task unused)
        {
            Thread.SpinWait(5);
        }
        var watch = Stopwatch.StartNew();
        for (int i = 0; i <= AsyncOpsQty; i++)
        {
            var t = db.StringSetAsync(key, i);
            if (withContinuation)
            {
                // Intentionally unawaited
                _ = t.ContinueWith(NonTrivial);
            }
        }
        Assert.Equal(AsyncOpsQty, await db.StringGetAsync(key).ForAwait());
        watch.Stop();
        Log($"{Me()}: Time for {AsyncOpsQty} ops: {watch.ElapsedMilliseconds}ms ({(withContinuation ? "with continuation" : "no continuation")}, any order); ops/s: {AsyncOpsQty / watch.Elapsed.TotalSeconds}");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(50)]
    public async Task MassiveBulkOpsSync(int threads)
    {
        Skip.UnlessLongRunning();
        await using var conn = Create(syncTimeout: 30000);

        RedisKey key = Me();
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        int workPerThread = SyncOpsQty / threads;
        var timeTaken = RunConcurrent(
            () =>
            {
                for (int i = 0; i < workPerThread; i++)
                {
                    db.StringIncrement(key, flags: CommandFlags.FireAndForget);
                }
            },
            threads);

        int val = (int)db.StringGet(key);
        Assert.Equal(workPerThread * threads, val);
        Log($"{Me()}: Time for {threads * workPerThread} ops on {threads} threads: {timeTaken.TotalMilliseconds}ms (any order); ops/s: {(workPerThread * threads) / timeTaken.TotalSeconds}");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    public async Task MassiveBulkOpsFireAndForget(int threads)
    {
        await using var conn = Create(syncTimeout: 30000);

        RedisKey key = Me();
        var db = conn.GetDatabase();
        await db.PingAsync();

        db.KeyDelete(key, CommandFlags.FireAndForget);
        int perThread = AsyncOpsQty / threads;
        var elapsed = RunConcurrent(
            () =>
            {
                for (int i = 0; i < perThread; i++)
                {
                    db.StringIncrement(key, flags: CommandFlags.FireAndForget);
                }
                db.Ping();
            },
            threads);
        var val = (long)db.StringGet(key);
        Assert.Equal(perThread * threads, val);

        Log($"{Me()}: Time for {val} ops over {threads} threads: {elapsed.TotalMilliseconds:###,###}ms (any order); ops/s: {val / elapsed.TotalSeconds:###,###,##0}");
    }
}
