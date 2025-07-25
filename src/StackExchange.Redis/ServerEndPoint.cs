﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using static StackExchange.Redis.PhysicalBridge;

namespace StackExchange.Redis
{
    [Flags]
    internal enum UnselectableFlags
    {
        None = 0,
        RedundantPrimary = 1,
        DidNotRespond = 2,
        ServerType = 4,
    }

    internal sealed partial class ServerEndPoint : IDisposable
    {
        internal volatile ServerEndPoint? Primary;
        internal volatile ServerEndPoint[] Replicas = Array.Empty<ServerEndPoint>();
        private static readonly Regex nameSanitizer = new Regex("[^!-~]+", RegexOptions.Compiled);

        private readonly Hashtable knownScripts = new Hashtable(StringComparer.Ordinal);

        private int databases, writeEverySeconds;
        private PhysicalBridge? interactive, subscription;
        private bool isDisposed, replicaReadOnly, isReplica, allowReplicaWrites;
        private bool? supportsDatabases, supportsPrimaryWrites;
        private ServerType serverType;
        private volatile UnselectableFlags unselectableReasons;
        private Version version;

        internal void ResetNonConnected()
        {
            interactive?.ResetNonConnected();
            subscription?.ResetNonConnected();
        }

        public ServerEndPoint(ConnectionMultiplexer multiplexer, EndPoint endpoint)
        {
            Multiplexer = multiplexer;
            EndPoint = endpoint;
            var config = multiplexer.RawConfig;
            version = config.DefaultVersion;
            replicaReadOnly = true;
            isReplica = false;
            databases = 0;
            writeEverySeconds = config.KeepAlive > 0 ? config.KeepAlive : 60;
            serverType = ServerType.Standalone;
            ConfigCheckSeconds = Multiplexer.RawConfig.ConfigCheckSeconds;

            // overrides for twemproxy/envoyproxy
            switch (multiplexer.RawConfig.Proxy)
            {
                case Proxy.Twemproxy:
                    databases = 1;
                    serverType = ServerType.Twemproxy;
                    break;
                case Proxy.Envoyproxy:
                    databases = 1;
                    serverType = ServerType.Envoyproxy;
                    break;
            }
        }

        public EndPoint EndPoint { get; }

        public ClusterConfiguration? ClusterConfiguration { get; private set; }

        /// <summary>
        /// Whether this endpoint supports databases at all.
        /// Note that some servers are cluster but present as standalone (e.g. Redis Enterprise), so we respect
        /// <see cref="RedisCommand.SELECT"/> being disabled here as a performance workaround.
        /// </summary>
        /// <remarks>
        /// This is memoized because it's accessed on hot paths inside the write lock.
        /// </remarks>
        public bool SupportsDatabases =>
            supportsDatabases ??= serverType == ServerType.Standalone && Multiplexer.CommandMap.IsAvailable(RedisCommand.SELECT);

        public int Databases
        {
            get => databases;
            set => SetConfig(ref databases, value);
        }

        public bool IsConnecting => interactive?.IsConnecting == true;
        public bool IsConnected => interactive?.IsConnected == true;
        public bool IsSubscriberConnected => KnowOrAssumeResp3() ? IsConnected : subscription?.IsConnected == true;

        public bool KnowOrAssumeResp3()
        {
            var protocol = interactive?.Protocol;
            return protocol is not null
                ? protocol.GetValueOrDefault() >= RedisProtocol.Resp3 // <= if we've completed handshake, use what we *know for sure*
                : Multiplexer.RawConfig.TryResp3(); // otherwise, use what we *expect*
        }

        public bool SupportsSubscriptions => Multiplexer.CommandMap.IsAvailable(RedisCommand.SUBSCRIBE);
        public bool SupportsPrimaryWrites => supportsPrimaryWrites ??= !IsReplica || !ReplicaReadOnly || AllowReplicaWrites;

        private readonly List<TaskCompletionSource<string>> _pendingConnectionMonitors = new List<TaskCompletionSource<string>>();

        /// <summary>
        /// Awaitable state seeing if this endpoint is connected.
        /// </summary>
        public Task<string> OnConnectedAsync(ILogger? log = null, bool sendTracerIfConnected = false, bool autoConfigureIfConnected = false)
        {
            async Task<string> IfConnectedAsync(ILogger? log, bool sendTracerIfConnected, bool autoConfigureIfConnected)
            {
                log?.LogInformationOnConnectedAsyncAlreadyConnectedStart(new(this));
                if (autoConfigureIfConnected)
                {
                    await AutoConfigureAsync(null, log).ForAwait();
                }
                if (sendTracerIfConnected)
                {
                    await SendTracerAsync(log).ForAwait();
                }
                log?.LogInformationOnConnectedAsyncAlreadyConnectedEnd(new(this));
                return "Already connected";
            }

            if (!IsConnected)
            {
                log?.LogInformationOnConnectedAsyncInit(new(this), interactive?.ConnectionState);
                var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
                _ = tcs.Task.ContinueWith(t => log?.LogInformationOnConnectedAsyncCompleted(new(this), t.Result));
                lock (_pendingConnectionMonitors)
                {
                    _pendingConnectionMonitors.Add(tcs);
                    // In case we complete in a race above, before attaching
                    if (IsConnected)
                    {
                        tcs.TrySetResult("Connection race");
                        _pendingConnectionMonitors.Remove(tcs);
                    }
                }
                return tcs.Task;
            }
            return IfConnectedAsync(log, sendTracerIfConnected, autoConfigureIfConnected);
        }

        internal Exception? LastException
        {
            get
            {
                var snapshot = interactive;
                var subEx = subscription?.LastException;
                var subExData = subEx?.Data;

                // check if subscription endpoint has a better last exception
                if (subExData != null && subExData.Contains("Redis-FailureType") && subExData["Redis-FailureType"]?.ToString() != nameof(ConnectionFailureType.UnableToConnect))
                {
                    return subEx;
                }
                return snapshot?.LastException;
            }
        }

        internal State InteractiveConnectionState => interactive?.ConnectionState ?? State.Disconnected;
        internal State SubscriptionConnectionState => KnowOrAssumeResp3() ? InteractiveConnectionState : subscription?.ConnectionState ?? State.Disconnected;

        public long OperationCount => interactive?.OperationCount ?? 0 + subscription?.OperationCount ?? 0;

        public bool RequiresReadMode => serverType == ServerType.Cluster && IsReplica;

        public ServerType ServerType
        {
            get => serverType;
            set => SetConfig(ref serverType, value);
        }

        public bool IsReplica
        {
            get => isReplica;
            set => SetConfig(ref isReplica, value);
        }

        public bool ReplicaReadOnly
        {
            get => replicaReadOnly;
            set => SetConfig(ref replicaReadOnly, value);
        }

        public bool AllowReplicaWrites
        {
            get => allowReplicaWrites;
            set
            {
                allowReplicaWrites = value;
                ClearMemoized();
            }
        }

        public Version Version
        {
            get => version;
            set => SetConfig(ref version, value);
        }

        /// <summary>
        /// If we have a connection (interactive), report the protocol being used.
        /// </summary>
        public RedisProtocol? Protocol => interactive?.Protocol;

        public int WriteEverySeconds
        {
            get => writeEverySeconds;
            set => SetConfig(ref writeEverySeconds, value);
        }

        internal ConnectionMultiplexer Multiplexer { get; }

        public void Dispose()
        {
            isDisposed = true;
            var tmp = interactive;
            interactive = null;
            tmp?.Dispose();

            tmp = subscription;
            subscription = null;
            tmp?.Dispose();
        }

        public PhysicalBridge? GetBridge(ConnectionType type, bool create = true, ILogger? log = null)
        {
            if (isDisposed) return null;
            switch (type)
            {
                case ConnectionType.Interactive:
                case ConnectionType.Subscription when KnowOrAssumeResp3():
                    return interactive ?? (create ? interactive = CreateBridge(ConnectionType.Interactive, log) : null);
                case ConnectionType.Subscription:
                    return subscription ?? (create ? subscription = CreateBridge(ConnectionType.Subscription, log) : null);
                default:
                    return null;
            }
        }

        public PhysicalBridge? GetBridge(Message message)
        {
            if (isDisposed) return null;

            // Subscription commands go to a specific bridge - so we need to set that up.
            // There are other commands we need to send to the right connection (e.g. subscriber PING with an explicit SetForSubscriptionBridge call),
            // but these always go subscriber.
            switch (message.Command)
            {
                case RedisCommand.SUBSCRIBE:
                case RedisCommand.UNSUBSCRIBE:
                case RedisCommand.PSUBSCRIBE:
                case RedisCommand.PUNSUBSCRIBE:
                case RedisCommand.SSUBSCRIBE:
                case RedisCommand.SUNSUBSCRIBE:
                    message.SetForSubscriptionBridge();
                    break;
            }

            return (message.IsForSubscriptionBridge && !KnowOrAssumeResp3())
                ? subscription ??= CreateBridge(ConnectionType.Subscription, null)
                : interactive ??= CreateBridge(ConnectionType.Interactive, null);
        }

        public PhysicalBridge? GetBridge(RedisCommand command, bool create = true)
        {
            if (isDisposed) return null;
            switch (command)
            {
                case RedisCommand.SUBSCRIBE:
                case RedisCommand.UNSUBSCRIBE:
                case RedisCommand.PSUBSCRIBE:
                case RedisCommand.PUNSUBSCRIBE:
                case RedisCommand.SSUBSCRIBE:
                case RedisCommand.SUNSUBSCRIBE:
                    if (!KnowOrAssumeResp3())
                    {
                        return subscription ?? (create ? subscription = CreateBridge(ConnectionType.Subscription, null) : null);
                    }
                    break;
            }
            return interactive ?? (create ? interactive = CreateBridge(ConnectionType.Interactive, null) : null);
        }

        public RedisFeatures GetFeatures() => new RedisFeatures(version);

        public void SetClusterConfiguration(ClusterConfiguration configuration)
        {
            ClusterConfiguration = configuration;

            if (configuration != null)
            {
                Multiplexer.Trace("Updating cluster ranges...");
                Multiplexer.UpdateClusterRange(configuration);
                Multiplexer.Trace("Resolving genealogy...");
                UpdateNodeRelations(configuration);
                Multiplexer.Trace("Cluster configured");
            }
        }

        public void UpdateNodeRelations(ClusterConfiguration configuration)
        {
            var thisNode = configuration.Nodes.FirstOrDefault(x => x.EndPoint?.Equals(EndPoint) == true);
            if (thisNode != null)
            {
                Multiplexer.Trace($"Updating node relations for {Format.ToString(thisNode.EndPoint)}...");
                List<ServerEndPoint>? replicas = null;
                ServerEndPoint? primary = null;
                foreach (var node in configuration.Nodes)
                {
                    if (node.NodeId == thisNode.ParentNodeId)
                    {
                        primary = Multiplexer.GetServerEndPoint(node.EndPoint);
                    }
                    else if (node.ParentNodeId == thisNode.NodeId && node.EndPoint is not null)
                    {
                        (replicas ??= new List<ServerEndPoint>()).Add(Multiplexer.GetServerEndPoint(node.EndPoint));
                    }
                }
                Primary = primary;
                Replicas = replicas?.ToArray() ?? Array.Empty<ServerEndPoint>();
            }
        }

        public void SetUnselectable(UnselectableFlags flags)
        {
            if (flags != 0)
            {
                var oldFlags = unselectableReasons;
                unselectableReasons |= flags;
                if (unselectableReasons != oldFlags)
                {
                    Multiplexer.Trace(unselectableReasons == 0 ? "Now usable" : ("Now unusable: " + flags), ToString());
                }
            }
        }

        public void ClearUnselectable(UnselectableFlags flags)
        {
            var oldFlags = unselectableReasons;
            if (oldFlags != 0)
            {
                unselectableReasons &= ~flags;
                if (unselectableReasons != oldFlags)
                {
                    Multiplexer.Trace(unselectableReasons == 0 ? "Now usable" : ("Now unusable: " + flags), ToString());
                }
            }
        }

        public override string ToString() => Format.ToString(EndPoint);

        [Obsolete("prefer async")]
        public WriteResult TryWriteSync(Message message) => GetBridge(message)?.TryWriteSync(message, isReplica) ?? WriteResult.NoConnectionAvailable;

        public ValueTask<WriteResult> TryWriteAsync(Message message) => GetBridge(message)?.TryWriteAsync(message, isReplica) ?? new ValueTask<WriteResult>(WriteResult.NoConnectionAvailable);

        internal void Activate(ConnectionType type, ILogger? log) => GetBridge(type, true, log);

        internal void AddScript(string script, byte[] hash)
        {
            lock (knownScripts)
            {
                knownScripts[script] = hash;
            }
        }

        internal async Task AutoConfigureAsync(PhysicalConnection? connection, ILogger? log = null)
        {
            if (!serverType.SupportsAutoConfigure())
            {
                // Don't try to detect configuration.
                // All the config commands are disabled and the fallback primary/replica detection won't help
                return;
            }

            log?.LogInformationAutoConfiguring(new(this));

            var commandMap = Multiplexer.CommandMap;
            const CommandFlags flags = CommandFlags.FireAndForget | CommandFlags.NoRedirect;
            var features = GetFeatures();
            Message msg;

            var autoConfigProcessor = ResultProcessor.AutoConfigureProcessor.Create(log);

            if (commandMap.IsAvailable(RedisCommand.CONFIG))
            {
                if (Multiplexer.RawConfig.KeepAlive <= 0)
                {
                    msg = Message.Create(-1, flags, RedisCommand.CONFIG, RedisLiterals.GET, RedisLiterals.timeout);
                    msg.SetInternalCall();
                    await WriteDirectOrQueueFireAndForgetAsync(connection, msg, autoConfigProcessor).ForAwait();
                }
                msg = Message.Create(-1, flags, RedisCommand.CONFIG, RedisLiterals.GET, features.ReplicaCommands ? RedisLiterals.replica_read_only : RedisLiterals.slave_read_only);
                msg.SetInternalCall();
                await WriteDirectOrQueueFireAndForgetAsync(connection, msg, autoConfigProcessor).ForAwait();
                msg = Message.Create(-1, flags, RedisCommand.CONFIG, RedisLiterals.GET, RedisLiterals.databases);
                msg.SetInternalCall();
                await WriteDirectOrQueueFireAndForgetAsync(connection, msg, autoConfigProcessor).ForAwait();
            }
            if (commandMap.IsAvailable(RedisCommand.SENTINEL))
            {
                msg = Message.Create(-1, flags, RedisCommand.SENTINEL, RedisLiterals.MASTERS);
                msg.SetInternalCall();
                await WriteDirectOrQueueFireAndForgetAsync(connection, msg, autoConfigProcessor).ForAwait();
            }
            if (commandMap.IsAvailable(RedisCommand.INFO))
            {
                lastInfoReplicationCheckTicks = Environment.TickCount;
                if (features.InfoSections)
                {
                    // note: Redis 7.0 has a multi-section usage, but we don't know
                    // the server version at this point; we *could* use the optional
                    // value on the config, but let's keep things simple: these
                    // commands are suitably cheap
                    msg = Message.Create(-1, flags, RedisCommand.INFO, RedisLiterals.replication);
                    msg.SetInternalCall();
                    await WriteDirectOrQueueFireAndForgetAsync(connection, msg, autoConfigProcessor).ForAwait();

                    msg = Message.Create(-1, flags, RedisCommand.INFO, RedisLiterals.server);
                    msg.SetInternalCall();
                    await WriteDirectOrQueueFireAndForgetAsync(connection, msg, autoConfigProcessor).ForAwait();
                }
                else
                {
                    msg = Message.Create(-1, flags, RedisCommand.INFO);
                    msg.SetInternalCall();
                    await WriteDirectOrQueueFireAndForgetAsync(connection, msg, autoConfigProcessor).ForAwait();
                }
            }
            else if (commandMap.IsAvailable(RedisCommand.SET))
            {
                // This is a nasty way to find if we are a replica, and it will only work on up-level servers, but...
                RedisKey key = Multiplexer.UniqueId;
                // The actual value here doesn't matter (we detect the error code if it fails).
                // The value here is to at least give some indication to anyone watching via "monitor",
                // but we could send two GUIDs (key/value) and it would work the same.
                msg = Message.Create(0, flags, RedisCommand.SET, key, RedisLiterals.replica_read_only, RedisLiterals.PX, 1, RedisLiterals.NX);
                msg.SetInternalCall();
                await WriteDirectOrQueueFireAndForgetAsync(connection, msg, autoConfigProcessor).ForAwait();
            }
            if (commandMap.IsAvailable(RedisCommand.CLUSTER))
            {
                msg = Message.Create(-1, flags, RedisCommand.CLUSTER, RedisLiterals.NODES);
                msg.SetInternalCall();
                await WriteDirectOrQueueFireAndForgetAsync(connection, msg, ResultProcessor.ClusterNodes).ForAwait();
            }
            // If we are going to fetch a tie breaker, do so last and we'll get it in before the tracer fires completing the connection
            // But if GETs are disabled on this, do not fail the connection - we just don't get tiebreaker benefits
            if (Multiplexer.RawConfig.TryGetTieBreaker(out var tieBreakerKey) && Multiplexer.CommandMap.IsAvailable(RedisCommand.GET))
            {
                log?.LogInformationRequestingTieBreak(new(EndPoint), tieBreakerKey);
                msg = Message.Create(0, flags, RedisCommand.GET, tieBreakerKey);
                msg.SetInternalCall();
                msg = LoggingMessage.Create(log, msg);
                await WriteDirectOrQueueFireAndForgetAsync(connection, msg, ResultProcessor.TieBreaker).ForAwait();
            }
        }

        private int _nextReplicaOffset;

        /// <summary>
        /// Used to round-robin between multiple replicas.
        /// </summary>
        internal uint NextReplicaOffset()
            => (uint)Interlocked.Increment(ref _nextReplicaOffset);

        internal Task Close(ConnectionType connectionType)
        {
            try
            {
                var tmp = GetBridge(connectionType, create: false);
                if (tmp == null || !tmp.IsConnected || !Multiplexer.CommandMap.IsAvailable(RedisCommand.QUIT))
                {
                    return Task.CompletedTask;
                }
                else
                {
                    return WriteDirectAsync(Message.Create(-1, CommandFlags.None, RedisCommand.QUIT), ResultProcessor.DemandOK, bridge: tmp);
                }
            }
            catch (Exception ex)
            {
                return Task.FromException(ex);
            }
        }

        internal void FlushScriptCache()
        {
            lock (knownScripts)
            {
                knownScripts.Clear();
            }
        }

        private string? runId;
        internal string? RunId
        {
            get => runId;
            set
            {
                // We only care about changes
                if (value != runId)
                {
                    // If we had an old run-id, and it has changed, then the server has been restarted
                    // ...which means the script cache is toast
                    if (runId != null)
                    {
                        FlushScriptCache();
                    }
                    runId = value;
                }
            }
        }

        internal ServerCounters GetCounters()
        {
            var counters = new ServerCounters(EndPoint);
            interactive?.GetCounters(counters.Interactive);
            subscription?.GetCounters(counters.Subscription);
            return counters;
        }

        internal BridgeStatus GetBridgeStatus(ConnectionType connectionType)
        {
            try
            {
                return GetBridge(connectionType, false)?.GetStatus() ?? BridgeStatus.Zero;
            }
            catch (Exception ex)
            {
                // only needs to be best efforts
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return BridgeStatus.Zero;
        }

        internal string GetProfile()
        {
            var sb = new StringBuilder(Format.ToString(EndPoint)).Append(": ");
            sb.Append("Circular op-count snapshot; int:");
            interactive?.AppendProfile(sb);
            sb.Append("; sub:");
            subscription?.AppendProfile(sb);
            return sb.ToString();
        }

        internal byte[]? GetScriptHash(string script, RedisCommand command)
        {
            var found = (byte[]?)knownScripts[script];
            if (found == null && command == RedisCommand.EVALSHA)
            {
                // The script provided is a hex SHA - store and re-use the ASCii for that
                found = Encoding.ASCII.GetBytes(script);
                lock (knownScripts)
                {
                    knownScripts[script] = found;
                }
            }
            return found;
        }

        internal string? GetStormLog(Message message) => GetBridge(message)?.GetStormLog();

        internal Message GetTracerMessage(bool checkResponse)
        {
            // Different configurations block certain commands, as can ad-hoc local configurations, so
            //   we'll do the best with what we have available.
            // Note: muxer-ctor asserts that one of ECHO, PING, TIME of GET is available
            // See also: TracerProcessor
            var map = Multiplexer.CommandMap;
            Message msg;
            const CommandFlags flags = CommandFlags.NoRedirect | CommandFlags.FireAndForget;
            if (checkResponse && map.IsAvailable(RedisCommand.ECHO))
            {
                msg = Message.Create(-1, flags, RedisCommand.ECHO, (RedisValue)Multiplexer.UniqueId);
            }
            else if (map.IsAvailable(RedisCommand.PING))
            {
                msg = Message.Create(-1, flags, RedisCommand.PING);
            }
            else if (map.IsAvailable(RedisCommand.TIME))
            {
                msg = Message.Create(-1, flags, RedisCommand.TIME);
            }
            else if (!checkResponse && map.IsAvailable(RedisCommand.ECHO))
            {
                // We'll use echo as a PING substitute if it is all we have (in preference to EXISTS)
                msg = Message.Create(-1, flags, RedisCommand.ECHO, (RedisValue)Multiplexer.UniqueId);
            }
            else
            {
                map.AssertAvailable(RedisCommand.EXISTS);
                msg = Message.Create(0, flags, RedisCommand.EXISTS, (RedisValue)Multiplexer.UniqueId);
            }
            msg.SetInternalCall();
            return msg;
        }

        internal UnselectableFlags GetUnselectableFlags() => unselectableReasons;

        internal bool IsSelectable(RedisCommand command, bool allowDisconnected = false)
        {
            // Until we've connected at least once, we're going to have a DidNotRespond unselectable reason present
            var bridge = unselectableReasons == 0 || (allowDisconnected && unselectableReasons == UnselectableFlags.DidNotRespond)
                ? GetBridge(command, false)
                : null;

            return bridge != null && (allowDisconnected || bridge.IsConnected);
        }

        private void CompletePendingConnectionMonitors(string source)
        {
            lock (_pendingConnectionMonitors)
            {
                foreach (var tcs in _pendingConnectionMonitors)
                {
                    tcs.TrySetResult(source);
                }
                _pendingConnectionMonitors.Clear();
            }
        }

        internal void OnDisconnected(PhysicalBridge bridge)
        {
            if (bridge == interactive)
            {
                CompletePendingConnectionMonitors("Disconnected");
                if (Protocol is RedisProtocol.Resp3)
                {
                    Multiplexer.UpdateSubscriptions();
                }
            }
            else if (bridge == subscription)
            {
                Multiplexer.UpdateSubscriptions();
            }
        }

        internal Task OnEstablishingAsync(PhysicalConnection connection, ILogger? log)
        {
            static async Task OnEstablishingAsyncAwaited(PhysicalConnection connection, Task handshake)
            {
                try
                {
                    await handshake.ForAwait();
                }
                catch (Exception ex)
                {
                    connection.RecordConnectionFailed(ConnectionFailureType.InternalFailure, ex);
                }
            }

            try
            {
                if (connection == null) return Task.CompletedTask;

                var handshake = HandshakeAsync(connection, log);

                if (handshake.Status != TaskStatus.RanToCompletion)
                {
                    return OnEstablishingAsyncAwaited(connection, handshake);
                }
            }
            catch (Exception ex)
            {
                connection.RecordConnectionFailed(ConnectionFailureType.InternalFailure, ex);
            }
            return Task.CompletedTask;
        }

        internal void OnFullyEstablished(PhysicalConnection connection, string source)
        {
            try
            {
                var bridge = connection?.BridgeCouldBeNull;
                if (bridge != null)
                {
                    // Clear the unselectable flag ASAP since we are open for business
                    ClearUnselectable(UnselectableFlags.DidNotRespond);

                    if (bridge == subscription)
                    {
                        // Note: this MUST be fire and forget, because we might be in the middle of a Sync processing
                        // TracerProcessor which is executing this line inside a SetResultCore().
                        // Since we're issuing commands inside a SetResult path in a message, we'd create a deadlock by waiting.
                        Multiplexer.EnsureSubscriptions(CommandFlags.FireAndForget);
                    }
                    if (IsConnected && (IsSubscriberConnected || !SupportsSubscriptions || KnowOrAssumeResp3()))
                    {
                        // Only connect on the second leg - we can accomplish this by checking both
                        // Or the first leg, if we're only making 1 connection because subscriptions aren't supported
                        CompletePendingConnectionMonitors(source);
                    }

                    Multiplexer.OnConnectionRestored(EndPoint, bridge.ConnectionType, connection?.ToString());
                }
            }
            catch (Exception ex)
            {
                connection?.RecordConnectionFailed(ConnectionFailureType.InternalFailure, ex);
            }
        }

        internal int LastInfoReplicationCheckSecondsAgo =>
            unchecked(Environment.TickCount - Thread.VolatileRead(ref lastInfoReplicationCheckTicks)) / 1000;

        private EndPoint? primaryEndPoint;
        public EndPoint? PrimaryEndPoint
        {
            get => primaryEndPoint;
            set => SetConfig(ref primaryEndPoint, value);
        }

        /// <summary>
        /// Result of the latest tie breaker (from the last reconfigure).
        /// </summary>
        internal string? TieBreakerResult { get; set; }

        internal bool CheckInfoReplication()
        {
            lastInfoReplicationCheckTicks = Environment.TickCount;
            ResetExponentiallyReplicationCheck();

            if (version.IsAtLeast(RedisFeatures.v2_8_0) && Multiplexer.CommandMap.IsAvailable(RedisCommand.INFO)
                && GetBridge(ConnectionType.Interactive, false) is PhysicalBridge bridge)
            {
                var msg = Message.Create(-1, CommandFlags.FireAndForget | CommandFlags.NoRedirect, RedisCommand.INFO, RedisLiterals.replication);
                msg.SetInternalCall();
                msg.SetSource(ResultProcessor.AutoConfigure, null);
#pragma warning disable CS0618 // Type or member is obsolete
                bridge.TryWriteSync(msg, isReplica);
#pragma warning restore CS0618
                return true;
            }
            return false;
        }

        private int lastInfoReplicationCheckTicks;
        internal volatile int ConfigCheckSeconds;
        [ThreadStatic]
        private static Random? r;

        /// <summary>
        /// Forces frequent replication check starting from 1 second up to max ConfigCheckSeconds with an exponential increment.
        /// </summary>
        internal void ForceExponentialBackoffReplicationCheck()
        {
            ConfigCheckSeconds = 1;
        }

        private void ResetExponentiallyReplicationCheck()
        {
            if (ConfigCheckSeconds < Multiplexer.RawConfig.ConfigCheckSeconds)
            {
                r ??= new Random();
                var newExponentialConfigCheck = ConfigCheckSeconds * 2;
                var jitter = r.Next(ConfigCheckSeconds + 1, newExponentialConfigCheck);
                ConfigCheckSeconds = Math.Min(jitter, Multiplexer.RawConfig.ConfigCheckSeconds);
            }
        }

        private int _heartBeatActive;
        internal void OnHeartbeat()
        {
            // Don't overlap heartbeat operations on an endpoint
            if (Interlocked.CompareExchange(ref _heartBeatActive, 1, 0) == 0)
            {
                try
                {
                    interactive?.OnHeartbeat(false);
                    subscription?.OnHeartbeat(false);
                }
                catch (Exception ex)
                {
                    Multiplexer.OnInternalError(ex, EndPoint);
                }
                finally
                {
                    Interlocked.Exchange(ref _heartBeatActive, 0);
                }
            }
        }

        internal Task<T?> WriteDirectAsync<T>(Message message, ResultProcessor<T> processor, PhysicalBridge? bridge = null)
        {
            static async Task<T?> Awaited(ServerEndPoint @this, Message message, ValueTask<WriteResult> write, TaskCompletionSource<T?> tcs)
            {
                var result = await write.ForAwait();
                if (result != WriteResult.Success)
                {
                    var ex = @this.Multiplexer.GetException(result, message, @this);
                    ConnectionMultiplexer.ThrowFailed(tcs, ex);
                }
                return await tcs.Task.ForAwait();
            }

            var source = TaskResultBox<T?>.Create(out var tcs, null);
            message.SetSource(processor, source);
            bridge ??= GetBridge(message);

            WriteResult result;
            if (bridge == null)
            {
                result = WriteResult.NoConnectionAvailable;
            }
            else
            {
                var write = bridge.TryWriteAsync(message, isReplica);
                if (!write.IsCompletedSuccessfully)
                {
                    return Awaited(this, message, write, tcs);
                }
                result = write.Result;
            }

            if (result != WriteResult.Success)
            {
                var ex = Multiplexer.GetException(result, message, this);
                ConnectionMultiplexer.ThrowFailed(tcs, ex);
            }
            return tcs.Task;
        }

        internal void ReportNextFailure()
        {
            interactive?.ReportNextFailure();
            subscription?.ReportNextFailure();
        }

        internal Task<bool> SendTracerAsync(ILogger? log = null)
        {
            var msg = GetTracerMessage(false);
            msg = LoggingMessage.Create(log, msg);
            return WriteDirectAsync(msg, ResultProcessor.Tracer);
        }

        internal string Summary()
        {
            var sb = new StringBuilder(Format.ToString(EndPoint))
                .Append(": ").Append(serverType).Append(" v").Append(version).Append(", ").Append(isReplica ? "replica" : "primary");

            if (databases > 0) sb.Append("; ").Append(databases).Append(" databases");
            if (writeEverySeconds > 0)
                sb.Append("; keep-alive: ").Append(TimeSpan.FromSeconds(writeEverySeconds));
            var tmp = interactive;
            sb.Append("; int: ").Append(tmp?.ConnectionState.ToString() ?? "n/a");
            tmp = subscription;
            if (tmp == null)
            {
                sb.Append("; sub: n/a");
            }
            else
            {
                var state = tmp.ConnectionState;
                sb.Append("; sub: ").Append(state);
                if (state == PhysicalBridge.State.ConnectedEstablished)
                {
                    sb.Append(", ").Append(tmp.SubscriptionCount).Append(" active");
                }
            }

            var flags = unselectableReasons;
            if (flags != 0)
            {
                sb.Append("; not in use: ").Append(flags);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Write the message directly to the pipe or fail...will not queue.
        /// </summary>
        /// <typeparam name="T">The type of the result processor.</typeparam>
        internal ValueTask WriteDirectOrQueueFireAndForgetAsync<T>(PhysicalConnection? connection, Message message, ResultProcessor<T> processor)
        {
            static async ValueTask Awaited(ValueTask<WriteResult> l_result) => await l_result.ForAwait();

            if (message != null)
            {
                message.SetSource(processor, null);
                ValueTask<WriteResult> result;
                if (connection == null)
                {
                    Multiplexer.Trace($"{Format.ToString(this)}: Enqueue (async): " + message);
                    // A bridge will be created if missing, so not nullable here
                    result = GetBridge(message)!.TryWriteAsync(message, isReplica);
                }
                else
                {
                    Multiplexer.Trace($"{Format.ToString(this)}: Writing direct (async): " + message);
                    var bridge = connection.BridgeCouldBeNull;
                    if (bridge == null)
                    {
                        throw new ObjectDisposedException(connection.ToString());
                    }
                    else
                    {
                        result = bridge.WriteMessageTakingWriteLockAsync(connection, message, bypassBacklog: true);
                    }
                }

                if (!result.IsCompletedSuccessfully)
                {
                    return Awaited(result);
                }
            }
            return default;
        }

        private PhysicalBridge? CreateBridge(ConnectionType type, ILogger? log)
        {
            if (Multiplexer.IsDisposed) return null;
            Multiplexer.Trace(type.ToString());
            var bridge = new PhysicalBridge(this, type, Multiplexer.TimeoutMilliseconds);
            bridge.TryConnect(log);
            return bridge;
        }

        private async Task HandshakeAsync(PhysicalConnection connection, ILogger? log)
        {
            log?.LogInformationServerHandshake(new(this));
            if (connection == null)
            {
                Multiplexer.Trace("No connection!?");
                return;
            }
            Message msg;
            // Note that we need "" (not null) for password in the case of 'nopass' logins
            var config = Multiplexer.RawConfig;
            string? user = config.User;
            string password = config.Password ?? "";

            string clientName = Multiplexer.ClientName;
            if (!string.IsNullOrWhiteSpace(clientName))
            {
                clientName = nameSanitizer.Replace(clientName, "");
            }

            // NOTE:
            // we might send the auth and client-name *twice* in RESP3 mode; this is intentional:
            // - we don't know for sure which commands are available; HELLO is not always available,
            //   even on v6 servers, and we don't usually even know the server version yet; likewise,
            //   CLIENT could be disabled/renamed
            // - on an authenticated server, you MUST issue HELLO with AUTH, so we can't avoid it there
            // - but if the HELLO with AUTH isn't recognized, we might still need to auth; the following is
            //   legal in all scenarios, and results in a consistent state:
            //
            //   (auth enabled)
            //
            //   HELLO 3 AUTH {user} {password} SETNAME {client}
            //   AUTH {user} {password}
            //   CLIENT SETNAME {client}
            //
            //   (auth disabled)
            //
            //   HELLO 3 SETNAME {client}
            //   CLIENT SETNAME {client}
            //
            // this might look a little redundant, but: we only do it once per connection, and it isn't
            // many bytes different; this allows us to pipeline the entire handshake without having to
            // add latency

            // note on the use of FireAndForget here; in F+F, the result processor is still invoked, which
            // is what we need for things to work; what *doesn't* happen is the result-box activation etc;
            // that's fine and doesn't cause a problem; if we wanted we could probably just discard (`_ =`)
            // the various tasks and just `return connection.FlushAsync();` - however, since handshake is low
            // volume, we can afford to optimize for a good stack-trace rather than avoiding state machines.
            ResultProcessor<bool>? autoConfig = null;
            if (Multiplexer.RawConfig.TryResp3()) // note this includes an availability check on HELLO
            {
                log?.LogInformationAuthenticatingViaHello(new(this));
                var hello = Message.CreateHello(3, user, password, clientName, CommandFlags.FireAndForget);
                hello.SetInternalCall();
                await WriteDirectOrQueueFireAndForgetAsync(connection, hello, autoConfig ??= ResultProcessor.AutoConfigureProcessor.Create(log)).ForAwait();

                // note that the server can reject RESP3 via either an -ERR response (HELLO not understood), or by simply saying "nope",
                // so we don't set the actual .Protocol until we process the result of the HELLO request
            }
            else
            {
                // if we're not even issuing HELLO, we're RESP2
                connection.SetProtocol(RedisProtocol.Resp2);
            }

            // note: we auth EVEN IF we have used HELLO to AUTH; because otherwise the fallback/detection path is pure hell,
            // and: we're pipelined here, so... meh
            if (!string.IsNullOrWhiteSpace(user) && Multiplexer.CommandMap.IsAvailable(RedisCommand.AUTH))
            {
                log?.LogInformationAuthenticatingUserPassword(new(this));
                msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.AUTH, (RedisValue)user, (RedisValue)password);
                msg.SetInternalCall();
                await WriteDirectOrQueueFireAndForgetAsync(connection, msg, ResultProcessor.DemandOK).ForAwait();
            }
            else if (!string.IsNullOrWhiteSpace(password) && Multiplexer.CommandMap.IsAvailable(RedisCommand.AUTH))
            {
                log?.LogInformationAuthenticatingPassword(new(this));
                msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.AUTH, (RedisValue)password);
                msg.SetInternalCall();
                await WriteDirectOrQueueFireAndForgetAsync(connection, msg, ResultProcessor.DemandOK).ForAwait();
            }

            if (Multiplexer.CommandMap.IsAvailable(RedisCommand.CLIENT))
            {
                if (!string.IsNullOrWhiteSpace(clientName))
                {
                    log?.LogInformationSettingClientName(new(this), clientName);
                    msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.CLIENT, RedisLiterals.SETNAME, (RedisValue)clientName);
                    msg.SetInternalCall();
                    await WriteDirectOrQueueFireAndForgetAsync(connection, msg, ResultProcessor.DemandOK).ForAwait();
                }

                if (config.SetClientLibrary)
                {
                    // note that this is a relatively new feature, but usually we won't know the
                    // server version, so we will use this speculatively and hope for the best
                    log?.LogInformationSettingClientLibVer(new(this));

                    var libName = Multiplexer.GetFullLibraryName();
                    if (!string.IsNullOrWhiteSpace(libName))
                    {
                        msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.CLIENT, RedisLiterals.SETINFO, RedisLiterals.lib_name, libName);
                        msg.SetInternalCall();
                        await WriteDirectOrQueueFireAndForgetAsync(connection, msg, ResultProcessor.DemandOK).ForAwait();
                    }

                    var version = ClientInfoSanitize(Utils.GetLibVersion());
                    if (!string.IsNullOrWhiteSpace(version))
                    {
                        msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.CLIENT, RedisLiterals.SETINFO, RedisLiterals.lib_ver, version);
                        msg.SetInternalCall();
                        await WriteDirectOrQueueFireAndForgetAsync(connection, msg, ResultProcessor.DemandOK).ForAwait();
                    }
                }

                msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.CLIENT, RedisLiterals.ID);
                msg.SetInternalCall();
                await WriteDirectOrQueueFireAndForgetAsync(connection, msg, autoConfig ??= ResultProcessor.AutoConfigureProcessor.Create(log)).ForAwait();
            }

            var bridge = connection.BridgeCouldBeNull;
            if (bridge is null)
            {
                return;
            }

            var connType = bridge.ConnectionType;
            if (connType == ConnectionType.Interactive)
            {
                await AutoConfigureAsync(connection, log).ForAwait();
            }

            var tracer = GetTracerMessage(true);
            tracer = LoggingMessage.Create(log, tracer);
            log?.LogInformationSendingCriticalTracer(new(this), tracer.CommandAndKey);
            await WriteDirectOrQueueFireAndForgetAsync(connection, tracer, ResultProcessor.EstablishConnection).ForAwait();

            // Note: this **must** be the last thing on the subscription handshake, because after this
            // we will be in subscriber mode: regular commands cannot be sent
            if (connType == ConnectionType.Subscription)
            {
                var configChannel = Multiplexer.ConfigurationChangedChannel;
                if (configChannel != null)
                {
                    msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.SUBSCRIBE, RedisChannel.Literal(configChannel));
                    // Note: this is NOT internal, we want it to queue in a backlog for sending when ready if necessary
                    await WriteDirectOrQueueFireAndForgetAsync(connection, msg, ResultProcessor.TrackSubscriptions).ForAwait();
                }
            }
            log?.LogInformationFlushingOutboundBuffer(new(this));
            await connection.FlushAsync().ForAwait();
        }

        private void SetConfig<T>(ref T field, T value, [CallerMemberName] string? caller = null)
        {
            if (!EqualityComparer<T>.Default.Equals(field, value))
            {
                // multiplexer might be null here in some test scenarios; just roll with it...
                Multiplexer?.Trace(caller + " changed from " + field + " to " + value, "Configuration");
                field = value;
                ClearMemoized();
                Multiplexer?.ReconfigureIfNeeded(EndPoint, false, caller!);
            }
        }
        internal static string ClientInfoSanitize(string? value)
            => string.IsNullOrWhiteSpace(value) ? "" : nameSanitizer.Replace(value!.Trim(), "-");

        private void ClearMemoized()
        {
            supportsDatabases = null;
            supportsPrimaryWrites = null;
        }

        /// <summary>
        /// For testing only.
        /// </summary>
        internal void SimulateConnectionFailure(SimulatedFailureType failureType)
        {
            interactive?.SimulateConnectionFailure(failureType);
            subscription?.SimulateConnectionFailure(failureType);
        }

        internal bool HasPendingCallerFacingItems()
        {
            // check whichever bridges exist
            if (interactive?.HasPendingCallerFacingItems() == true) return true;
            return subscription?.HasPendingCallerFacingItems() ?? false;
        }
    }
}
