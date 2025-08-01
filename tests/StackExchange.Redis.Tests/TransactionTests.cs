﻿using System;
using System.Threading.Tasks;
using Xunit;

namespace StackExchange.Redis.Tests;

[RunPerProtocol]
public class TransactionTests(ITestOutputHelper output, SharedConnectionFixture fixture) : TestBase(output, fixture)
{
    [Fact]
    public async Task BasicEmptyTran()
    {
        await using var conn = Create();

        RedisKey key = Me();
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));

        var tran = db.CreateTransaction();

        var result = tran.Execute();
        Assert.True(result);
    }

    [Fact]
    public async Task NestedTransactionThrows()
    {
        await using var conn = Create();

        var db = conn.GetDatabase();
        var tran = db.CreateTransaction();
        var redisTransaction = Assert.IsType<RedisTransaction>(tran);
        Assert.Throws<NotSupportedException>(() => redisTransaction.CreateTransaction(null));
    }

    [Theory]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    [InlineData(true, true, true)]
    public async Task BasicTranWithExistsCondition(bool demandKeyExists, bool keyExists, bool expectTranResult)
    {
        await using var conn = Create(disabledCommands: ["info", "config"]);

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);
        if (keyExists) db.StringSet(key2, "any value", flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(keyExists, db.KeyExists(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(demandKeyExists ? Condition.KeyExists(key2) : Condition.KeyNotExists(key2));
        var incr = tran.StringIncrementAsync(key);
        var exec = tran.ExecuteAsync();
        var get = db.StringGet(key);

        Assert.Equal(expectTranResult, await exec);
        if (demandKeyExists == keyExists)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    [Theory]
    [InlineData("same", "same", true, true)]
    [InlineData("x", "y", true, false)]
    [InlineData("x", null, true, false)]
    [InlineData(null, "y", true, false)]
    [InlineData(null, null, true, true)]

    [InlineData("same", "same", false, false)]
    [InlineData("x", "y", false, true)]
    [InlineData("x", null, false, true)]
    [InlineData(null, "y", false, true)]
    [InlineData(null, null, false, false)]
    public async Task BasicTranWithEqualsCondition(string? expected, string? value, bool expectEqual, bool expectTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        if (value != null) db.StringSet(key2, value, flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(value, db.StringGet(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(expectEqual ? Condition.StringEqual(key2, expected) : Condition.StringNotEqual(key2, expected));
        var incr = tran.StringIncrementAsync(key);
        var exec = tran.ExecuteAsync();
        var get = db.StringGet(key);

        Assert.Equal(expectTranResult, await exec);
        if (expectEqual == (value == expected))
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    [Theory]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    [InlineData(true, true, true)]
    public async Task BasicTranWithHashExistsCondition(bool demandKeyExists, bool keyExists, bool expectTranResult)
    {
        await using var conn = Create(disabledCommands: ["info", "config"]);

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);
        RedisValue hashField = "field";
        if (keyExists) db.HashSet(key2, hashField, "any value", flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(keyExists, db.HashExists(key2, hashField));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(demandKeyExists ? Condition.HashExists(key2, hashField) : Condition.HashNotExists(key2, hashField));
        var incr = tran.StringIncrementAsync(key);
        var exec = tran.ExecuteAsync();
        var get = db.StringGet(key);

        Assert.Equal(expectTranResult, await exec);
        if (demandKeyExists == keyExists)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    [Theory]
    [InlineData("same", "same", true, true)]
    [InlineData("x", "y", true, false)]
    [InlineData("x", null, true, false)]
    [InlineData(null, "y", true, false)]
    [InlineData(null, null, true, true)]

    [InlineData("same", "same", false, false)]
    [InlineData("x", "y", false, true)]
    [InlineData("x", null, false, true)]
    [InlineData(null, "y", false, true)]
    [InlineData(null, null, false, false)]
    public async Task BasicTranWithHashEqualsCondition(string? expected, string? value, bool expectEqual, bool expectedTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        RedisValue hashField = "field";
        if (value != null) db.HashSet(key2, hashField, value, flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(value, db.HashGet(key2, hashField));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(expectEqual ? Condition.HashEqual(key2, hashField, expected) : Condition.HashNotEqual(key2, hashField, expected));
        var incr = tran.StringIncrementAsync(key);
        var exec = tran.ExecuteAsync();
        var get = db.StringGet(key);

        Assert.Equal(expectedTranResult, await exec);
        if (expectEqual == (value == expected))
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    private static TaskStatus SafeStatus(Task task)
    {
        if (task.Status == TaskStatus.WaitingForActivation)
        {
            try
            {
                if (!task.Wait(1000)) throw new TimeoutException("timeout waiting for task to complete");
            }
            catch (AggregateException ex)
            when (ex.InnerException is TaskCanceledException
                || (ex.InnerExceptions.Count == 1 && ex.InnerException is TaskCanceledException))
            {
                return TaskStatus.Canceled;
            }
            catch (TaskCanceledException)
            {
                return TaskStatus.Canceled;
            }
        }
        return task.Status;
    }

    [Theory]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    [InlineData(true, true, true)]
    public async Task BasicTranWithListExistsCondition(bool demandKeyExists, bool keyExists, bool expectTranResult)
    {
        await using var conn = Create(disabledCommands: ["info", "config"]);

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);
        if (keyExists) db.ListRightPush(key2, "any value", flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(keyExists, db.KeyExists(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(demandKeyExists ? Condition.ListIndexExists(key2, 0) : Condition.ListIndexNotExists(key2, 0));
        var push = tran.ListRightPushAsync(key, "any value");
        var exec = tran.ExecuteAsync();
        var get = db.ListGetByIndex(key, 0);

        Assert.Equal(expectTranResult, await exec);
        if (demandKeyExists == keyExists)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await push); // eq: push
            Assert.Equal("any value", get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(push)); // neq: push
            Assert.Null((string?)get); // neq: get
        }
    }

    [Theory]
    [InlineData("same", "same", true, true)]
    [InlineData("x", "y", true, false)]
    [InlineData("x", null, true, false)]
    [InlineData(null, "y", true, false)]
    [InlineData(null, null, true, true)]

    [InlineData("same", "same", false, false)]
    [InlineData("x", "y", false, true)]
    [InlineData("x", null, false, true)]
    [InlineData(null, "y", false, true)]
    [InlineData(null, null, false, false)]
    public async Task BasicTranWithListEqualsCondition(string? expected, string? value, bool expectEqual, bool expectTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        if (value != null) db.ListRightPush(key2, value, flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(value, db.ListGetByIndex(key2, 0));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(expectEqual ? Condition.ListIndexEqual(key2, 0, expected) : Condition.ListIndexNotEqual(key2, 0, expected));
        var push = tran.ListRightPushAsync(key, "any value");
        var exec = tran.ExecuteAsync();
        var get = db.ListGetByIndex(key, 0);

        Assert.Equal(expectTranResult, await exec);
        if (expectEqual == (value == expected))
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await push); // eq: push
            Assert.Equal("any value", get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(push)); // neq: push
            Assert.Null((string?)get); // neq: get
        }
    }

    public enum ComparisonType
    {
        Equal,
        LessThan,
        GreaterThan,
    }

    [Theory]
    [InlineData("five", ComparisonType.Equal, 5L, false)]
    [InlineData("four", ComparisonType.Equal, 4L, true)]
    [InlineData("three", ComparisonType.Equal, 3L, false)]
    [InlineData("", ComparisonType.Equal, 2L, false)]
    [InlineData("", ComparisonType.Equal, 0L, true)]
    [InlineData(null, ComparisonType.Equal, 1L, false)]
    [InlineData(null, ComparisonType.Equal, 0L, true)]

    [InlineData("five", ComparisonType.LessThan, 5L, true)]
    [InlineData("four", ComparisonType.LessThan, 4L, false)]
    [InlineData("three", ComparisonType.LessThan, 3L, false)]
    [InlineData("", ComparisonType.LessThan, 2L, true)]
    [InlineData("", ComparisonType.LessThan, 0L, false)]
    [InlineData(null, ComparisonType.LessThan, 1L, true)]
    [InlineData(null, ComparisonType.LessThan, 0L, false)]

    [InlineData("five", ComparisonType.GreaterThan, 5L, false)]
    [InlineData("four", ComparisonType.GreaterThan, 4L, false)]
    [InlineData("three", ComparisonType.GreaterThan, 3L, true)]
    [InlineData("", ComparisonType.GreaterThan, 2L, false)]
    [InlineData("", ComparisonType.GreaterThan, 0L, false)]
    [InlineData(null, ComparisonType.GreaterThan, 1L, false)]
    [InlineData(null, ComparisonType.GreaterThan, 0L, false)]
    public async Task BasicTranWithStringLengthCondition(string? value, ComparisonType type, long length, bool expectTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        bool expectSuccess;
        Condition? condition;
        var valueLength = value?.Length ?? 0;
        switch (type)
        {
            case ComparisonType.Equal:
                expectSuccess = valueLength == length;
                condition = Condition.StringLengthEqual(key2, length);
                Assert.Contains("String length == " + length, condition.ToString());
                break;
            case ComparisonType.GreaterThan:
                expectSuccess = valueLength > length;
                condition = Condition.StringLengthGreaterThan(key2, length);
                Assert.Contains("String length > " + length, condition.ToString());
                break;
            case ComparisonType.LessThan:
                expectSuccess = valueLength < length;
                condition = Condition.StringLengthLessThan(key2, length);
                Assert.Contains("String length < " + length, condition.ToString());
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }

        if (value != null) db.StringSet(key2, value, flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(value, db.StringGet(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(condition);
        var push = tran.StringSetAsync(key, "any value");
        var exec = tran.ExecuteAsync();
        var get = db.StringLength(key);

        Assert.Equal(expectTranResult, await exec);

        if (expectSuccess)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.True(await push); // eq: push
            Assert.Equal("any value".Length, get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(push)); // neq: push
            Assert.Equal(0, get); // neq: get
        }
    }

    [Theory]
    [InlineData("five", ComparisonType.Equal, 5L, false)]
    [InlineData("four", ComparisonType.Equal, 4L, true)]
    [InlineData("three", ComparisonType.Equal, 3L, false)]
    [InlineData("", ComparisonType.Equal, 2L, false)]
    [InlineData("", ComparisonType.Equal, 0L, true)]

    [InlineData("five", ComparisonType.LessThan, 5L, true)]
    [InlineData("four", ComparisonType.LessThan, 4L, false)]
    [InlineData("three", ComparisonType.LessThan, 3L, false)]
    [InlineData("", ComparisonType.LessThan, 2L, true)]
    [InlineData("", ComparisonType.LessThan, 0L, false)]

    [InlineData("five", ComparisonType.GreaterThan, 5L, false)]
    [InlineData("four", ComparisonType.GreaterThan, 4L, false)]
    [InlineData("three", ComparisonType.GreaterThan, 3L, true)]
    [InlineData("", ComparisonType.GreaterThan, 2L, false)]
    [InlineData("", ComparisonType.GreaterThan, 0L, false)]
    public async Task BasicTranWithHashLengthCondition(string value, ComparisonType type, long length, bool expectTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        bool expectSuccess;
        Condition? condition;
        var valueLength = value?.Length ?? 0;
        switch (type)
        {
            case ComparisonType.Equal:
                expectSuccess = valueLength == length;
                condition = Condition.HashLengthEqual(key2, length);
                break;
            case ComparisonType.GreaterThan:
                expectSuccess = valueLength > length;
                condition = Condition.HashLengthGreaterThan(key2, length);
                break;
            case ComparisonType.LessThan:
                expectSuccess = valueLength < length;
                condition = Condition.HashLengthLessThan(key2, length);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }

        for (var i = 0; i < valueLength; i++)
        {
            db.HashSet(key2, i, value![i].ToString(), flags: CommandFlags.FireAndForget);
        }
        Assert.False(db.KeyExists(key));
        Assert.Equal(valueLength, db.HashLength(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(condition);
        var push = tran.StringSetAsync(key, "any value");
        var exec = tran.ExecuteAsync();
        var get = db.StringLength(key);

        Assert.Equal(expectTranResult, await exec);

        if (expectSuccess)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.True(await push); // eq: push
            Assert.Equal("any value".Length, get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(push)); // neq: push
            Assert.Equal(0, get); // neq: get
        }
    }

    [Theory]
    [InlineData("five", ComparisonType.Equal, 5L, false)]
    [InlineData("four", ComparisonType.Equal, 4L, true)]
    [InlineData("three", ComparisonType.Equal, 3L, false)]
    [InlineData("", ComparisonType.Equal, 2L, false)]
    [InlineData("", ComparisonType.Equal, 0L, true)]

    [InlineData("five", ComparisonType.LessThan, 5L, true)]
    [InlineData("four", ComparisonType.LessThan, 4L, false)]
    [InlineData("three", ComparisonType.LessThan, 3L, false)]
    [InlineData("", ComparisonType.LessThan, 2L, true)]
    [InlineData("", ComparisonType.LessThan, 0L, false)]

    [InlineData("five", ComparisonType.GreaterThan, 5L, false)]
    [InlineData("four", ComparisonType.GreaterThan, 4L, false)]
    [InlineData("three", ComparisonType.GreaterThan, 3L, true)]
    [InlineData("", ComparisonType.GreaterThan, 2L, false)]
    [InlineData("", ComparisonType.GreaterThan, 0L, false)]
    public async Task BasicTranWithSetCardinalityCondition(string value, ComparisonType type, long length, bool expectTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        bool expectSuccess;
        Condition? condition;
        var valueLength = value?.Length ?? 0;
        switch (type)
        {
            case ComparisonType.Equal:
                expectSuccess = valueLength == length;
                condition = Condition.SetLengthEqual(key2, length);
                break;
            case ComparisonType.GreaterThan:
                expectSuccess = valueLength > length;
                condition = Condition.SetLengthGreaterThan(key2, length);
                break;
            case ComparisonType.LessThan:
                expectSuccess = valueLength < length;
                condition = Condition.SetLengthLessThan(key2, length);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }

        for (var i = 0; i < valueLength; i++)
        {
            db.SetAdd(key2, i, flags: CommandFlags.FireAndForget);
        }
        Assert.False(db.KeyExists(key));
        Assert.Equal(valueLength, db.SetLength(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(condition);
        var push = tran.StringSetAsync(key, "any value");
        var exec = tran.ExecuteAsync();
        var get = db.StringLength(key);

        Assert.Equal(expectTranResult, await exec);

        if (expectSuccess)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.True(await push); // eq: push
            Assert.Equal("any value".Length, get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(push)); // neq: push
            Assert.Equal(0, get); // neq: get
        }
    }

    [Theory]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    [InlineData(true, true, true)]
    public async Task BasicTranWithSetContainsCondition(bool demandKeyExists, bool keyExists, bool expectTranResult)
    {
        await using var conn = Create(disabledCommands: ["info", "config"]);

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);
        RedisValue member = "value";
        if (keyExists) db.SetAdd(key2, member, flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(keyExists, db.SetContains(key2, member));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(demandKeyExists ? Condition.SetContains(key2, member) : Condition.SetNotContains(key2, member));
        var incr = tran.StringIncrementAsync(key);
        var exec = tran.ExecuteAsync();
        var get = db.StringGet(key);

        Assert.Equal(expectTranResult, await exec);
        if (demandKeyExists == keyExists)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    [Theory]
    [InlineData("five", ComparisonType.Equal, 5L, false)]
    [InlineData("four", ComparisonType.Equal, 4L, true)]
    [InlineData("three", ComparisonType.Equal, 3L, false)]
    [InlineData("", ComparisonType.Equal, 2L, false)]
    [InlineData("", ComparisonType.Equal, 0L, true)]

    [InlineData("five", ComparisonType.LessThan, 5L, true)]
    [InlineData("four", ComparisonType.LessThan, 4L, false)]
    [InlineData("three", ComparisonType.LessThan, 3L, false)]
    [InlineData("", ComparisonType.LessThan, 2L, true)]
    [InlineData("", ComparisonType.LessThan, 0L, false)]

    [InlineData("five", ComparisonType.GreaterThan, 5L, false)]
    [InlineData("four", ComparisonType.GreaterThan, 4L, false)]
    [InlineData("three", ComparisonType.GreaterThan, 3L, true)]
    [InlineData("", ComparisonType.GreaterThan, 2L, false)]
    [InlineData("", ComparisonType.GreaterThan, 0L, false)]
    public async Task BasicTranWithSortedSetCardinalityCondition(string value, ComparisonType type, long length, bool expectTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        bool expectSuccess;
        Condition? condition;
        var valueLength = value?.Length ?? 0;
        switch (type)
        {
            case ComparisonType.Equal:
                expectSuccess = valueLength == length;
                condition = Condition.SortedSetLengthEqual(key2, length);
                break;
            case ComparisonType.GreaterThan:
                expectSuccess = valueLength > length;
                condition = Condition.SortedSetLengthGreaterThan(key2, length);
                break;
            case ComparisonType.LessThan:
                expectSuccess = valueLength < length;
                condition = Condition.SortedSetLengthLessThan(key2, length);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }

        for (var i = 0; i < valueLength; i++)
        {
            db.SortedSetAdd(key2, i, i, flags: CommandFlags.FireAndForget);
        }
        Assert.False(db.KeyExists(key));
        Assert.Equal(valueLength, db.SortedSetLength(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(condition);
        var push = tran.StringSetAsync(key, "any value");
        var exec = tran.ExecuteAsync();
        var get = db.StringLength(key);

        Assert.Equal(expectTranResult, await exec);

        if (expectSuccess)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.True(await push); // eq: push
            Assert.Equal("any value".Length, get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(push)); // neq: push
            Assert.Equal(0, get); // neq: get
        }
    }

    [Theory]
    [InlineData(1, 4, ComparisonType.Equal, 5L, false)]
    [InlineData(1, 4, ComparisonType.Equal, 4L, true)]
    [InlineData(1, 2, ComparisonType.Equal, 3L, false)]
    [InlineData(1, 1, ComparisonType.Equal, 2L, false)]
    [InlineData(0, 0, ComparisonType.Equal, 0L, false)]

    [InlineData(1, 4, ComparisonType.LessThan, 5L, true)]
    [InlineData(1, 4, ComparisonType.LessThan, 4L, false)]
    [InlineData(1, 3, ComparisonType.LessThan, 3L, false)]
    [InlineData(1, 1, ComparisonType.LessThan, 2L, true)]
    [InlineData(0, 0, ComparisonType.LessThan, 0L, false)]

    [InlineData(1, 5, ComparisonType.GreaterThan, 5L, false)]
    [InlineData(1, 4, ComparisonType.GreaterThan, 4L, false)]
    [InlineData(1, 4, ComparisonType.GreaterThan, 3L, true)]
    [InlineData(1, 2, ComparisonType.GreaterThan, 2L, false)]
    [InlineData(0, 0, ComparisonType.GreaterThan, 0L, true)]
    public async Task BasicTranWithSortedSetRangeCountCondition(double min, double max, ComparisonType type, long length, bool expectTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        bool expectSuccess;
        Condition? condition;
        var valueLength = (int)(max - min) + 1;
        switch (type)
        {
            case ComparisonType.Equal:
                expectSuccess = valueLength == length;
                condition = Condition.SortedSetLengthEqual(key2, length, min, max);
                break;
            case ComparisonType.GreaterThan:
                expectSuccess = valueLength > length;
                condition = Condition.SortedSetLengthGreaterThan(key2, length, min, max);
                break;
            case ComparisonType.LessThan:
                expectSuccess = valueLength < length;
                condition = Condition.SortedSetLengthLessThan(key2, length, min, max);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }

        for (var i = 0; i < 5; i++)
        {
            db.SortedSetAdd(key2, i, i, flags: CommandFlags.FireAndForget);
        }
        Assert.False(db.KeyExists(key));
        Assert.Equal(5, db.SortedSetLength(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(condition);
        var push = tran.StringSetAsync(key, "any value");
        var exec = tran.ExecuteAsync();
        var get = db.StringLength(key);

        Assert.Equal(expectTranResult, await exec);

        if (expectSuccess)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.True(await push); // eq: push
            Assert.Equal("any value".Length, get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(push)); // neq: push
            Assert.Equal(0, get); // neq: get
        }
    }

    [Theory]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    [InlineData(true, true, true)]
    public async Task BasicTranWithSortedSetContainsCondition(bool demandKeyExists, bool keyExists, bool expectTranResult)
    {
        await using var conn = Create(disabledCommands: ["info", "config"]);

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);
        RedisValue member = "value";
        if (keyExists) db.SortedSetAdd(key2, member, 0.0, flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(keyExists, db.SortedSetScore(key2, member).HasValue);

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(demandKeyExists ? Condition.SortedSetContains(key2, member) : Condition.SortedSetNotContains(key2, member));
        var incr = tran.StringIncrementAsync(key);
        var exec = tran.ExecuteAsync();
        var get = db.StringGet(key);

        Assert.Equal(expectTranResult, await exec);
        if (demandKeyExists == keyExists)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    public enum SortedSetValue
    {
        None,
        Exact,
        Shorter,
        Longer,
    }

    [Theory]
    [InlineData(false, SortedSetValue.None, true)]
    [InlineData(false, SortedSetValue.Shorter, true)]
    [InlineData(false, SortedSetValue.Exact, false)]
    [InlineData(false, SortedSetValue.Longer, false)]
    [InlineData(true, SortedSetValue.None, false)]
    [InlineData(true, SortedSetValue.Shorter, false)]
    [InlineData(true, SortedSetValue.Exact, true)]
    [InlineData(true, SortedSetValue.Longer, true)]
    public async Task BasicTranWithSortedSetStartsWithCondition_String(bool requestExists, SortedSetValue existingValue, bool expectTranResult)
    {
        using var conn = Create();

        RedisKey key1 = Me() + "_1", key2 = Me() + "_2";
        var db = conn.GetDatabase();
        db.KeyDelete(key1, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        db.SortedSetAdd(key2, "unrelated", 0.0, flags: CommandFlags.FireAndForget);
        switch (existingValue)
        {
            case SortedSetValue.Shorter:
                db.SortedSetAdd(key2, "see", 0.0, flags: CommandFlags.FireAndForget);
                break;
            case SortedSetValue.Exact:
                db.SortedSetAdd(key2, "seek", 0.0, flags: CommandFlags.FireAndForget);
                break;
            case SortedSetValue.Longer:
                db.SortedSetAdd(key2, "seeks", 0.0, flags: CommandFlags.FireAndForget);
                break;
        }

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(requestExists ? Condition.SortedSetContainsStarting(key2, "seek") : Condition.SortedSetNotContainsStarting(key2, "seek"));
        var incr = tran.StringIncrementAsync(key1);
        var exec = await tran.ExecuteAsync();
        var get = await db.StringGetAsync(key1);

        Assert.Equal(expectTranResult, exec);
        Assert.Equal(expectTranResult, cond.WasSatisfied);

        if (expectTranResult)
        {
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    [Theory]
    [InlineData(false, SortedSetValue.None, true)]
    [InlineData(false, SortedSetValue.Shorter, true)]
    [InlineData(false, SortedSetValue.Exact, false)]
    [InlineData(false, SortedSetValue.Longer, false)]
    [InlineData(true, SortedSetValue.None, false)]
    [InlineData(true, SortedSetValue.Shorter, false)]
    [InlineData(true, SortedSetValue.Exact, true)]
    [InlineData(true, SortedSetValue.Longer, true)]
    public async Task BasicTranWithSortedSetStartsWithCondition_Integer(bool requestExists, SortedSetValue existingValue, bool expectTranResult)
    {
        using var conn = Create();

        RedisKey key1 = Me() + "_1", key2 = Me() + "_2";
        var db = conn.GetDatabase();
        db.KeyDelete(key1, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        db.SortedSetAdd(key2, 789, 0.0, flags: CommandFlags.FireAndForget);
        switch (existingValue)
        {
            case SortedSetValue.Shorter:
                db.SortedSetAdd(key2, 123, 0.0, flags: CommandFlags.FireAndForget);
                break;
            case SortedSetValue.Exact:
                db.SortedSetAdd(key2, 1234, 0.0, flags: CommandFlags.FireAndForget);
                break;
            case SortedSetValue.Longer:
                db.SortedSetAdd(key2, 12345, 0.0, flags: CommandFlags.FireAndForget);
                break;
        }

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(requestExists ? Condition.SortedSetContainsStarting(key2, 1234) : Condition.SortedSetNotContainsStarting(key2, 1234));
        var incr = tran.StringIncrementAsync(key1);
        var exec = await tran.ExecuteAsync();
        var get = await db.StringGetAsync(key1);

        Assert.Equal(expectTranResult, exec);
        Assert.Equal(expectTranResult, cond.WasSatisfied);

        if (expectTranResult)
        {
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    [Theory]
    [InlineData(4D, 4D, true, true)]
    [InlineData(4D, 5D, true, false)]
    [InlineData(4D, null, true, false)]
    [InlineData(null, 5D, true, false)]
    [InlineData(null, null, true, true)]

    [InlineData(4D, 4D, false, false)]
    [InlineData(4D, 5D, false, true)]
    [InlineData(4D, null, false, true)]
    [InlineData(null, 5D, false, true)]
    [InlineData(null, null, false, false)]
    public async Task BasicTranWithSortedSetEqualCondition(double? expected, double? value, bool expectEqual, bool expectedTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        RedisValue member = "member";
        if (value != null) db.SortedSetAdd(key2, member, value.Value, flags: CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));
        Assert.Equal(value, db.SortedSetScore(key2, member));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(expectEqual ? Condition.SortedSetEqual(key2, member, expected) : Condition.SortedSetNotEqual(key2, member, expected));
        var incr = tran.StringIncrementAsync(key);
        var exec = tran.ExecuteAsync();
        var get = db.StringGet(key);

        Assert.Equal(expectedTranResult, await exec);
        if (expectEqual == (value == expected))
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    [Theory]
    [InlineData(true, true, true, true)]
    [InlineData(true, false, true, true)]
    [InlineData(false, true, true, true)]
    [InlineData(true, true, false, false)]
    [InlineData(true, false, false, false)]
    [InlineData(false, true, false, false)]
    [InlineData(false, false, true, false)]
    [InlineData(false, false, false, true)]
    public async Task BasicTranWithSortedSetScoreExistsCondition(bool member1HasScore, bool member2HasScore, bool demandScoreExists, bool expectedTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        const double Score = 4D;
        RedisValue member1 = "member1";
        RedisValue member2 = "member2";
        if (member1HasScore)
        {
            db.SortedSetAdd(key2, member1, Score, flags: CommandFlags.FireAndForget);
        }

        if (member2HasScore)
        {
            db.SortedSetAdd(key2, member2, Score, flags: CommandFlags.FireAndForget);
        }

        Assert.False(db.KeyExists(key));
        Assert.Equal(member1HasScore ? Score : null, db.SortedSetScore(key2, member1));
        Assert.Equal(member2HasScore ? Score : null, db.SortedSetScore(key2, member2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(demandScoreExists ? Condition.SortedSetScoreExists(key2, Score) : Condition.SortedSetScoreNotExists(key2, Score));
        var incr = tran.StringIncrementAsync(key);
        var exec = tran.ExecuteAsync();
        var get = db.StringGet(key);

        Assert.Equal(expectedTranResult, await exec);
        if ((member1HasScore || member2HasScore) == demandScoreExists)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    [Theory]
    [InlineData(true, true, 2L, true, true)]
    [InlineData(true, true, 2L, false, false)]
    [InlineData(true, true, 1L, true, false)]
    [InlineData(true, true, 1L, false, true)]
    [InlineData(true, false, 2L, true, false)]
    [InlineData(true, false, 2L, false, true)]
    [InlineData(true, false, 1L, true, true)]
    [InlineData(true, false, 1L, false, false)]
    [InlineData(false, true, 2L, true, false)]
    [InlineData(false, true, 2L, false, true)]
    [InlineData(false, true, 1L, true, true)]
    [InlineData(false, true, 1L, false, false)]
    [InlineData(false, false, 2L, true, false)]
    [InlineData(false, false, 2L, false, true)]
    [InlineData(false, false, 1L, true, false)]
    [InlineData(false, false, 1L, false, true)]
    public async Task BasicTranWithSortedSetScoreCountExistsCondition(bool member1HasScore, bool member2HasScore, long expectedLength, bool expectEqual, bool expectedTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        const double Score = 4D;
        var length = 0L;
        RedisValue member1 = "member1";
        RedisValue member2 = "member2";
        if (member1HasScore)
        {
            db.SortedSetAdd(key2, member1, Score, flags: CommandFlags.FireAndForget);
            length++;
        }

        if (member2HasScore)
        {
            db.SortedSetAdd(key2, member2, Score, flags: CommandFlags.FireAndForget);
            length++;
        }

        Assert.False(db.KeyExists(key));
        Assert.Equal(length, db.SortedSetLength(key2, Score, Score));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(expectEqual ? Condition.SortedSetScoreExists(key2, Score, expectedLength) : Condition.SortedSetScoreNotExists(key2, Score, expectedLength));
        var incr = tran.StringIncrementAsync(key);
        var exec = tran.ExecuteAsync();
        var get = db.StringGet(key);

        Assert.Equal(expectedTranResult, await exec);
        if (expectEqual == (length == expectedLength))
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.Equal(1, await incr); // eq: incr
            Assert.Equal(1, (long)get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(incr)); // neq: incr
            Assert.Equal(0, (long)get); // neq: get
        }
    }

    [Theory]
    [InlineData("five", ComparisonType.Equal, 5L, false)]
    [InlineData("four", ComparisonType.Equal, 4L, true)]
    [InlineData("three", ComparisonType.Equal, 3L, false)]
    [InlineData("", ComparisonType.Equal, 2L, false)]
    [InlineData("", ComparisonType.Equal, 0L, true)]

    [InlineData("five", ComparisonType.LessThan, 5L, true)]
    [InlineData("four", ComparisonType.LessThan, 4L, false)]
    [InlineData("three", ComparisonType.LessThan, 3L, false)]
    [InlineData("", ComparisonType.LessThan, 2L, true)]
    [InlineData("", ComparisonType.LessThan, 0L, false)]

    [InlineData("five", ComparisonType.GreaterThan, 5L, false)]
    [InlineData("four", ComparisonType.GreaterThan, 4L, false)]
    [InlineData("three", ComparisonType.GreaterThan, 3L, true)]
    [InlineData("", ComparisonType.GreaterThan, 2L, false)]
    [InlineData("", ComparisonType.GreaterThan, 0L, false)]
    public async Task BasicTranWithListLengthCondition(string value, ComparisonType type, long length, bool expectTranResult)
    {
        await using var conn = Create();

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        bool expectSuccess;
        Condition? condition;
        var valueLength = value?.Length ?? 0;
        switch (type)
        {
            case ComparisonType.Equal:
                expectSuccess = valueLength == length;
                condition = Condition.ListLengthEqual(key2, length);
                break;
            case ComparisonType.GreaterThan:
                expectSuccess = valueLength > length;
                condition = Condition.ListLengthGreaterThan(key2, length);
                break;
            case ComparisonType.LessThan:
                expectSuccess = valueLength < length;
                condition = Condition.ListLengthLessThan(key2, length);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }

        for (var i = 0; i < valueLength; i++)
        {
            db.ListRightPush(key2, i, flags: CommandFlags.FireAndForget);
        }
        Assert.False(db.KeyExists(key));
        Assert.Equal(valueLength, db.ListLength(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(condition);
        var push = tran.StringSetAsync(key, "any value");
        var exec = tran.ExecuteAsync();
        var get = db.StringLength(key);

        Assert.Equal(expectTranResult, await exec);

        if (expectSuccess)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.True(await push); // eq: push
            Assert.Equal("any value".Length, get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(push)); // neq: push
            Assert.Equal(0, get); // neq: get
        }
    }

    [Theory]
    [InlineData("five", ComparisonType.Equal, 5L, false)]
    [InlineData("four", ComparisonType.Equal, 4L, true)]
    [InlineData("three", ComparisonType.Equal, 3L, false)]
    [InlineData("", ComparisonType.Equal, 2L, false)]
    [InlineData("", ComparisonType.Equal, 0L, true)]

    [InlineData("five", ComparisonType.LessThan, 5L, true)]
    [InlineData("four", ComparisonType.LessThan, 4L, false)]
    [InlineData("three", ComparisonType.LessThan, 3L, false)]
    [InlineData("", ComparisonType.LessThan, 2L, true)]
    [InlineData("", ComparisonType.LessThan, 0L, false)]

    [InlineData("five", ComparisonType.GreaterThan, 5L, false)]
    [InlineData("four", ComparisonType.GreaterThan, 4L, false)]
    [InlineData("three", ComparisonType.GreaterThan, 3L, true)]
    [InlineData("", ComparisonType.GreaterThan, 2L, false)]
    [InlineData("", ComparisonType.GreaterThan, 0L, false)]
    public async Task BasicTranWithStreamLengthCondition(string value, ComparisonType type, long length, bool expectTranResult)
    {
        await using var conn = Create(require: RedisFeatures.v5_0_0);

        RedisKey key = Me(), key2 = Me() + "2";
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        db.KeyDelete(key2, CommandFlags.FireAndForget);

        bool expectSuccess;
        Condition? condition;
        var valueLength = value?.Length ?? 0;
        switch (type)
        {
            case ComparisonType.Equal:
                expectSuccess = valueLength == length;
                condition = Condition.StreamLengthEqual(key2, length);
                break;
            case ComparisonType.GreaterThan:
                expectSuccess = valueLength > length;
                condition = Condition.StreamLengthGreaterThan(key2, length);
                break;
            case ComparisonType.LessThan:
                expectSuccess = valueLength < length;
                condition = Condition.StreamLengthLessThan(key2, length);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }
        RedisValue fieldName = "Test";
        for (var i = 0; i < valueLength; i++)
        {
            db.StreamAdd(key2, fieldName, i, flags: CommandFlags.FireAndForget);
        }
        Assert.False(db.KeyExists(key));
        Assert.Equal(valueLength, db.StreamLength(key2));

        var tran = db.CreateTransaction();
        var cond = tran.AddCondition(condition);
        var push = tran.StringSetAsync(key, "any value");
        var exec = tran.ExecuteAsync();
        var get = db.StringLength(key);

        Assert.Equal(expectTranResult, await exec);

        if (expectSuccess)
        {
            Assert.True(await exec, "eq: exec");
            Assert.True(cond.WasSatisfied, "eq: was satisfied");
            Assert.True(await push); // eq: push
            Assert.Equal("any value".Length, get); // eq: get
        }
        else
        {
            Assert.False(await exec, "neq: exec");
            Assert.False(cond.WasSatisfied, "neq: was satisfied");
            Assert.Equal(TaskStatus.Canceled, SafeStatus(push)); // neq: push
            Assert.Equal(0, get); // neq: get
        }
    }

    [Fact]
    public async Task BasicTran()
    {
        await using var conn = Create();

        RedisKey key = Me();
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));

        var tran = db.CreateTransaction();
        var a = tran.StringIncrementAsync(key, 10);
        var b = tran.StringIncrementAsync(key, 5);
        var c = tran.StringGetAsync(key);
        var d = tran.KeyExistsAsync(key);
        var e = tran.KeyDeleteAsync(key);
        var f = tran.KeyExistsAsync(key);
        Assert.False(a.IsCompleted);
        Assert.False(b.IsCompleted);
        Assert.False(c.IsCompleted);
        Assert.False(d.IsCompleted);
        Assert.False(e.IsCompleted);
        Assert.False(f.IsCompleted);
        var result = await tran.ExecuteAsync().ForAwait();
        Assert.True(result, "result");
        await Task.WhenAll(a, b, c, d, e, f).ForAwait();
        Assert.True(a.IsCompleted, "a");
        Assert.True(b.IsCompleted, "b");
        Assert.True(c.IsCompleted, "c");
        Assert.True(d.IsCompleted, "d");
        Assert.True(e.IsCompleted, "e");
        Assert.True(f.IsCompleted, "f");

        var g = db.KeyExists(key);

        Assert.Equal(10, await a.ForAwait());
        Assert.Equal(15, await b.ForAwait());
        Assert.Equal(15, (long)await c.ForAwait());
        Assert.True(await d.ForAwait());
        Assert.True(await e.ForAwait());
        Assert.False(await f.ForAwait());
        Assert.False(g);
    }

    [Fact]
    public async Task CombineFireAndForgetAndRegularAsyncInTransaction()
    {
        await using var conn = Create();

        RedisKey key = Me();
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));

        var tran = db.CreateTransaction("state");
        var a = tran.StringIncrementAsync(key, 5);
        var b = tran.StringIncrementAsync(key, 10, CommandFlags.FireAndForget);
        var c = tran.StringIncrementAsync(key, 15);
        Assert.True(tran.Execute());
        var count = (long)db.StringGet(key);

        Assert.Equal(5, await a);
        Assert.Equal("state", a.AsyncState);
        Assert.Equal(0, await b);
        Assert.Null(b.AsyncState);
        Assert.Equal(30, await c);
        Assert.Equal("state", a.AsyncState);
        Assert.Equal(30, count);
    }

    [Fact]
    public async Task TransactionWithAdHocCommandsAndSelectDisabled()
    {
        await using var conn = Create(disabledCommands: ["SELECT"]);
        RedisKey key = Me();
        var db = conn.GetDatabase();
        db.KeyDelete(key, CommandFlags.FireAndForget);
        Assert.False(db.KeyExists(key));

        var tran = db.CreateTransaction("state");
        var a = tran.ExecuteAsync("SET", "foo", "bar");
        Assert.True(await tran.ExecuteAsync());
        await a;
        var setting = db.StringGet("foo");
        Assert.Equal("bar", setting);
    }

#if VERBOSE
    [Fact]
    public async Task WatchAbort_StringEqual()
    {
        await using var vicConn = Create();
        await using var perpConn = Create();

        var key = Me();
        var db = vicConn.GetDatabase();

        // expect foo, change to bar at the last minute
        vicConn.PreTransactionExec += cmd =>
        {
            Writer.WriteLine($"'{cmd}' detected; changing it...");
            perpConn.GetDatabase().StringSet(key, "bar");
        };
        db.KeyDelete(key);
        db.StringSet(key, "foo");
        var tran = db.CreateTransaction();
        tran.AddCondition(Condition.StringEqual(key, "foo"));
        var pong = tran.PingAsync();
        Assert.False(await tran.ExecuteAsync(), "expected abort");
        await Assert.ThrowsAsync<TaskCanceledException>(() => pong);
    }

    [Fact]
    public async Task WatchAbort_HashLengthEqual()
    {
        await using var vicConn = Create();
        await using var perpConn = Create();

        var key = Me();
        var db = vicConn.GetDatabase();

        // expect foo, change to bar at the last minute
        vicConn.PreTransactionExec += cmd =>
        {
            Writer.WriteLine($"'{cmd}' detected; changing it...");
            perpConn.GetDatabase().HashSet(key, "bar", "def");
        };
        db.KeyDelete(key);
        db.HashSet(key, "foo", "abc");
        var tran = db.CreateTransaction();
        tran.AddCondition(Condition.HashLengthEqual(key, 1));
        var pong = tran.PingAsync();
        Assert.False(await tran.ExecuteAsync());
        await Assert.ThrowsAsync<TaskCanceledException>(() => pong);
    }
#endif

    [Fact]
    public async Task ExecCompletes_Issue943()
    {
        Skip.UnlessLongRunning();
        int hashHit = 0, hashMiss = 0, expireHit = 0, expireMiss = 0;
        await using (var conn = Create())
        {
            var db = conn.GetDatabase();
            for (int i = 0; i < 40000; i++)
            {
                RedisKey key = Me();
                await db.KeyDeleteAsync(key);
                HashEntry[] hashEntries =
                [
                    new HashEntry("blah", DateTime.UtcNow.ToString("R")),
                ];
                ITransaction transaction = db.CreateTransaction();
                transaction.AddCondition(Condition.KeyNotExists(key));
                Task hashSetTask = transaction.HashSetAsync(key, hashEntries);
                Task<bool> expireTask = transaction.KeyExpireAsync(key, TimeSpan.FromSeconds(30));
                bool committed = await transaction.ExecuteAsync();
                if (committed)
                {
                    if (hashSetTask.IsCompleted) hashHit++; else hashMiss++;
                    if (expireTask.IsCompleted) expireHit++; else expireMiss++;
                    await hashSetTask;
                    await expireTask;
                }
            }
        }

        Log($"hash hit: {hashHit}, miss: {hashMiss}; expire hit: {expireHit}, miss: {expireMiss}");
        Assert.Equal(0, hashMiss);
        Assert.Equal(0, expireMiss);
    }
}
