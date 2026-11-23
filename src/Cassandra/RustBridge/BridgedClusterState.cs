using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Net;
using System.Runtime.CompilerServices;
using Cassandra.Serialization;
using static Cassandra.RustBridge;

namespace Cassandra
{
    internal sealed class BridgedClusterState : RustResource
    {
        internal BridgedClusterState(ManuallyDestructible mdClusterState) : base(mdClusterState)
        {
        }

        internal bool Equals(BridgedClusterState other)
        {
            if (other is null) return false;
            return handle == other.handle;
        }

        [StructLayout(LayoutKind.Sequential)]
        struct CSharpHostData
        {
            public FFISliceRaw IdBytes;
            public FFISliceRaw IpBytes;
            public ushort Port;
            public FFIString Datacenter;
            public FFIString Rack;
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIMaybeException cluster_state_fill_nodes(
            IntPtr clusterState,
            IntPtr contextPtr,
            IntPtr callback);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, CSharpHostData, FFIMaybeException> AddHostPtr = &AddHostToList;
        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIMaybeException cluster_state_get_replicas_legacy_murmur3(
            IntPtr clusterStatePtr,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspace,
            RustBridge.FFISlice<byte> partitionKey,
            IntPtr callbackContext,
            IntPtr callback,
            IntPtr constructors);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIMaybeException cluster_state_get_replicas(
            IntPtr clusterStatePtr,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspace,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string table,
            IntPtr populateValuesContext,
            IntPtr populateValuesCallback,
            IntPtr callbackState,
            IntPtr callback,
            IntPtr constructors);

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException AddHostToList(
            IntPtr contextPtr,
            CSharpHostData hostData)
        {
            try
            {
                // Safety:
                // contextPtr is the address of a StackSlot in FillHostCache. The slot holds the 'context' reference.
                // The address stays valid during this callback:
                // 1. cluster_state_fill_nodes calls this callback synchronously, before it returns.
                // 2. The slot is a ref struct, thus it is on the stack of FillHostCache, which is still running.
                // 3. If the GC moves the RefreshContext object, the GC updates the reference in the slot.
                // See StackSlot<T> for the full explanation.
                var context = Unsafe.AsRef<Metadata.RefreshContext>((void*)contextPtr);

                var hostId = GuidFromFFIFormat(hostData.IdBytes.As<byte>().ToSpan());

                // Construct IPAddress directly from bytes (4 for IPv4, 16 for IPv6). ipBytes is an FFISlice<byte>
                // and it accesses unmanaged memory that is only valid for the duration of this callback invocation.
                // The IPAddress constructor must be called synchronously here so it can copy the data immediately.
                var ipAddress = new IPAddress(hostData.IpBytes.As<byte>().ToSpan());
                var address = new IPEndPoint(ipAddress, hostData.Port);

                // Try to reuse existing host object if id matches and address is the same
                if (context.OldHosts != null && context.OldHosts.TryGetValue(hostId, out var host))
                {
                    // If the address matches, reuse the instance.
                    if (host.Address.Equals(address))
                    {
                        context.AddHost(host);
                        return FFIMaybeException.Ok();
                    }
                }

                // If either datacenter or rack is null, the method ToManagedString returns null.
                var dcString = hostData.Datacenter.ToManagedString();
                var rackString = hostData.Rack.ToManagedString();

                // Create Host instance and add it to the dictionaries.
                host = new Host(address, hostId, dcString, rackString);
                context.AddHost(host);
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        internal void FillHostCache(Metadata.RefreshContext context)
        {
            var contextSlot = new StackSlot<Metadata.RefreshContext>(context);
            var contextPtr = contextSlot.AddressOf();
            unsafe
            {
                RunWithIncrement(handle =>
                    cluster_state_fill_nodes(
                        handle,
                        contextPtr,
                        (IntPtr)AddHostPtr
                    )
                );
            }
        }

        [StructLayout(LayoutKind.Sequential)]
        private readonly struct ReplicaPair
        {
            // Points to a 16-byte UUID (Rust side: *const [u8; 16]).
            public readonly IntPtr HostIdBytesPtr;
            public readonly uint Shard;
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, ReplicaPair, RustBridge.FFIMaybeException> OnReplicaPairPtr = &OnReplicaPairCallback;

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe RustBridge.FFIMaybeException OnReplicaPairCallback(IntPtr contextPtr, ReplicaPair replica)
        {
            try
            {
                var context = Unsafe.AsRef<GetReplicasContext>((void*)contextPtr);

                const int HostIdLength = 16;
                var hostIdBytes = new ReadOnlySpan<byte>((void*)replica.HostIdBytesPtr, HostIdLength);
                var hostId = GuidFromFFIFormat(hostIdBytes);

                if (context.HostsById.TryGetValue(hostId, out var host))
                {
                    context.AddReplica(new HostShard(host, (int)replica.Shard));
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Retrieved an unrecognized Replica: hostId={hostId}, shard={replica.Shard}");
                }

                return RustBridge.FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                return RustBridge.FFIMaybeException.FromException(ex);
            }
        }

        private class GetReplicasContext(IReadOnlyDictionary<Guid, Host> hostsById)
        {
            private readonly List<HostShard> _replicas = [];
            internal IReadOnlyDictionary<Guid, Host> HostsById { get; } = hostsById;

            internal void AddReplica(HostShard hostShard) => _replicas.Add(hostShard);
            internal ICollection<HostShard> Replicas => _replicas;
        }

        internal ICollection<HostShard> GetReplicasLegacyMurmur3(
            string keyspace, IReadOnlyDictionary<Guid, Host> hostsById, byte[] partitionKey)
        {
            var contextSlot = new StackSlot<GetReplicasContext>(new(hostsById));
            var contextPtr = contextSlot.AddressOf();

            unsafe
            {
                fixed (byte* partitionKeyPtr = partitionKey)
                {
                    var partitionKeySlice = new FFISlice<byte>(
                        (IntPtr)partitionKeyPtr,
                        (nuint)partitionKey.Length
                    );
                    RunWithIncrement(ptr => cluster_state_get_replicas_legacy_murmur3(
                        ptr,
                        keyspace,
                        partitionKeySlice,
                        contextPtr,
                        (IntPtr)OnReplicaPairPtr,
                        (IntPtr)Globals.ConstructorsPtr
                    ));
                }
            }

            return contextSlot.Value.Replicas;
        }

        /// <summary>
        /// Gets replicas using table-aware routing.
        /// Each partition key value is serialized individually via the populate-callback pattern.
        /// </summary>
        internal unsafe ICollection<HostShard> GetReplicas(
            string keyspace,
            string table,
            IReadOnlyDictionary<Guid, Host> hostsById,
            IReadOnlyList<object> partitionKeyValues,
            ISerializer serializer)
        {
            var contextSlot = new StackSlot<GetReplicasContext>(new(hostsById));
            var contextPtr = contextSlot.AddressOf();
            var populateCtxSlot = new StackSlot<PopulateValuesContext>(
                SerializationHandler.CreateContext(partitionKeyValues, serializer));
            var populateCtxPtr = populateCtxSlot.AddressOf();

            RunWithIncrement(ptr => cluster_state_get_replicas(
                ptr,
                keyspace,
                table,
                populateCtxPtr,
                (IntPtr)SerializationHandler.PopulateValuesPtr,
                contextPtr,
                (IntPtr)OnReplicaPairPtr,
                (IntPtr)Globals.ConstructorsPtr
            ));

            return contextSlot.Value.Replicas;
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException cluster_state_get_keyspace_metadata(
            IntPtr clusterState,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspaceName,
            IntPtr contextPtr,
            IntPtr replicationOptionsPtr,
            StrategyAddRepFactorCallbacks addRepFactorCallbacks,
            IntPtr callback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, nuint, FFIMaybeException> SimpleStrategyAddRepFactorPtr = &SimpleStrategyAddRepFactor;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException SimpleStrategyAddRepFactor(
            IntPtr replicationOptionsPtr,
            nuint repFactor)
        {
            try
            {
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);
                replicationOptions["replication_factor"] = repFactor.ToString();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, nuint, FFIMaybeException> NetworkTopologyStrategyAddRepFactorPtr = &NetworkTopologyStrategyAddRepFactor;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException NetworkTopologyStrategyAddRepFactor(
            IntPtr replicationOptionsPtr,
            FFIString datacenter,
            nuint repFactor)
        {
            try
            {
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);
                replicationOptions[datacenter.ToManagedString()] = repFactor.ToString();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIString, FFIMaybeException> OtherStrategyAddRepFactorPtr = &OtherStrategyAddRepFactor;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException OtherStrategyAddRepFactor(
            IntPtr replicationOptionsPtr,
            FFIString strategyClass,
            FFIString repFactor)
        {
            try
            {
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);
                replicationOptions[strategyClass.ToManagedString()] = repFactor.ToManagedString();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        [StructLayout(LayoutKind.Sequential)]
        private unsafe readonly struct StrategyAddRepFactorCallbacks
        {
            public readonly IntPtr SimpleStrategyCallback;
            public readonly IntPtr NetworkTopologyStrategyCallback;
            public readonly IntPtr OtherStrategyCallback;

            public StrategyAddRepFactorCallbacks()
            {
                SimpleStrategyCallback = (IntPtr)SimpleStrategyAddRepFactorPtr;
                NetworkTopologyStrategyCallback = (IntPtr)NetworkTopologyStrategyAddRepFactorPtr;
                OtherStrategyCallback = (IntPtr)OtherStrategyAddRepFactorPtr;
            }
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIBool, FFIString, IntPtr, FFIMaybeException> FillKeyspaceMetadataPtr = &FillKeyspaceMetadata;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException FillKeyspaceMetadata(
            IntPtr contextPtr,
            FFIBool durableWrites,
            FFIString strategyClass,
            IntPtr replicationOptionsPtr)
        {
            try
            {
                var keyspaceMeta = Unsafe.AsRef<KeyspaceMetadata>((void*)contextPtr);
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);

                var replication = new Dictionary<string, int>();
                foreach (var option in replicationOptions)
                {
                    if (int.TryParse(option.Value, out var parsedValue))
                    {
                        replication[option.Key] = parsedValue;
                    }
                    else
                    {
                        throw new InvalidOperationException($"Failed to parse replication option value '{option.Value}' for key '{option.Key}' into an integer.");
                    }
                }

                keyspaceMeta.FillKeyspaceMetadata(durableWrites, strategyClass.ToManagedString(), replication);
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        internal KeyspaceMetadata GetKeyspaceMetadata(string keyspaceName)
        {
            var ksmdSlot = new StackSlot<KeyspaceMetadata>(new(this, keyspaceName));
            var ksmdPtr = ksmdSlot.AddressOf();
            var replicationOptionsSlot = new StackSlot<Dictionary<string, string>>(new());
            var replicationOptionsPtr = replicationOptionsSlot.AddressOf();
            var addRepFactorCallbacks = new StrategyAddRepFactorCallbacks();
            try
            {
                unsafe
                {
                    RunWithIncrement(handle =>
                        cluster_state_get_keyspace_metadata(
                            handle,
                            keyspaceName,
                            ksmdPtr,
                            replicationOptionsPtr,
                            addRepFactorCallbacks,
                            (IntPtr)FillKeyspaceMetadataPtr,
                            (IntPtr)Globals.ConstructorsPtr
                        )
                    );
                }
            }
            catch (InvalidArgumentException)
            {
                // If the keyspace was not found return null.
                return null;
            }
            catch (Exception ex)
            {
                // For other exceptions, rethrow as they indicate a failure in metadata retrieval rather than missing keyspace.
                throw new InvalidOperationException($"Error retrieving metadata for keyspace '{keyspaceName}'.", ex);
            }

            return ksmdSlot.Value;
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException cluster_state_get_keyspace_names(
            IntPtr clusterState,
            IntPtr keyspaceNameListPtr,
            IntPtr callback);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIMaybeException> AddKeyspaceNamePtr = &AddKeyspaceName;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException AddKeyspaceName(
            IntPtr keyspaceNameListPtr,
            FFIString keyspaceName)
        {
            try
            {
                var keyspaceNameList = Unsafe.AsRef<List<string>>((void*)keyspaceNameListPtr);
                keyspaceNameList.Add(keyspaceName.ToManagedString());
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        internal List<string> GetKeyspaceNames()
        {
            var keyspaceNamesSlot = new StackSlot<List<string>>(new());
            var keyspaceNamesPtr = keyspaceNamesSlot.AddressOf();

            unsafe
            {
                RunWithIncrement(handle =>
                    cluster_state_get_keyspace_names(
                        handle,
                        keyspaceNamesPtr,
                        (IntPtr)AddKeyspaceNamePtr
                    )
                );
            }

            return keyspaceNamesSlot.Value;
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException cluster_state_get_table_names(
            IntPtr clusterState,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspaceName,
            IntPtr tableNameListPtr,
            IntPtr callback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIMaybeException> AddTableNamesPtr = &AddTableName;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException AddTableName(
            IntPtr tableNameListPtr,
            FFIString tableName)
        {
            try
            {
                var tableNameList = Unsafe.AsRef<List<string>>((void*)tableNameListPtr);
                tableNameList.Add(tableName.ToManagedString());
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        internal List<string> GetTableNames(string keyspaceName)
        {
            var tableNamesSlot = new StackSlot<List<string>>(new());
            var tableNamesPtr = tableNamesSlot.AddressOf();

            try
            {
                unsafe
                {
                    RunWithIncrement(handle =>
                        cluster_state_get_table_names(
                            handle,
                            keyspaceName,
                            tableNamesPtr,
                            (IntPtr)AddTableNamesPtr,
                            (IntPtr)Globals.ConstructorsPtr
                        )
                    );
                }
            }
            catch (InvalidArgumentException)
            {
                // If the keyspace was not found return null.
                return null;
            }
            catch (Exception ex)
            {
                // For other exceptions, rethrow as they indicate a failure in metadata retrieval rather than missing keyspace.
                throw new InvalidOperationException($"Error retrieving metadata for keyspace '{keyspaceName}'.", ex);
            }

            return tableNamesSlot.Value;
        }

        /// <summary>
        /// Represents the context used during asynchronous retrieval of UDT metadata,
        /// allowing to accumulate field definitions and construct
        /// the final UdtColumnInfo object once all data is received from Rust.
        /// </summary>
        private sealed class UdtContext
        {
            internal string KeyspaceName;
            internal List<ColumnDesc> Fields;
            internal UdtColumnInfo UdtDefinition;

            internal UdtContext(string keyspaceName)
            {
                KeyspaceName = keyspaceName;
                Fields = new List<ColumnDesc>();
            }
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException cluster_state_get_udt_metadata(
            IntPtr clusterState,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspaceName,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string udtName,
            IntPtr constructUdtFieldCallback,
            IntPtr udtContextPtr,
            IntPtr constructUdtMetadataCallback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, byte, IntPtr, FFIMaybeException> ConstructUdtFieldPtr = &ConstructUdtField;

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException ConstructUdtField(
            IntPtr udtContextPtr,
            FFIString fieldName,
            byte typeCode,
            IntPtr typeInfoPtr)
        {
            try
            {
                var udtContext = Unsafe.AsRef<UdtContext>((void*)udtContextPtr);

                var cqlTypeCode = (ColumnTypeCode)typeCode;
                var field = new ColumnDesc
                {
                    Name = fieldName.ToManagedString(),
                    TypeCode = cqlTypeCode,
                    TypeInfo = typeInfoPtr != IntPtr.Zero ? BridgedRowSet.BuildTypeInfoFromHandle(typeInfoPtr, cqlTypeCode, udtContext.KeyspaceName) : null
                };
                udtContext.Fields.Add(field);

                return FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIMaybeException> FillUdtMetadataPtr = &FillUdtMetadata;

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException FillUdtMetadata(
            IntPtr udtContextPtr,
            FFIString udtName)
        {
            try
            {
                var udtContext = Unsafe.AsRef<UdtContext>((void*)udtContextPtr);

                var udtDefinition = new UdtColumnInfo($"{udtContext.KeyspaceName}.{udtName.ToManagedString()}");
                foreach (var field in udtContext.Fields)
                {
                    udtDefinition.Fields.Add(field);
                }
                udtContext.UdtDefinition = udtDefinition;
                return FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }
        }

        internal UdtColumnInfo GetUdtMetadata(string keyspaceName, string udtName)
        {
            var udtContextSlot = new StackSlot<UdtContext>(new(keyspaceName));
            var udtContextPtr = udtContextSlot.AddressOf();

            try
            {
                unsafe
                {
                    RunWithIncrement(handle =>
                        cluster_state_get_udt_metadata(
                            handle,
                            keyspaceName,
                            udtName,
                            (IntPtr)ConstructUdtFieldPtr,
                            udtContextPtr,
                            (IntPtr)FillUdtMetadataPtr,
                            (IntPtr)Globals.ConstructorsPtr
                        ));
                }
            }
            catch (InvalidArgumentException)
            {
                return null;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error retrieving metadata for UDT '{udtName}' in keyspace '{keyspaceName}'.", ex);
            }

            return udtContextSlot.Value.UdtDefinition;
        }

        private sealed class TableColumnsContext
        {
            internal string KeyspaceName;
            internal List<TableColumn> Columns;

            internal TableColumnsContext(string keyspaceName)
            {
                KeyspaceName = keyspaceName;
                Columns = new List<TableColumn>();
            }
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, byte, IntPtr, FFIBool, FFIBool, FFIMaybeException> ConstructTableColumnPtr = &ConstructTableColumn;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException ConstructTableColumn(
            IntPtr tableColumnsContextPtr,
            FFIString columnName,
            byte typeCode,
            IntPtr typeInfoPtr,
            FFIBool isStatic,
            FFIBool isFrozen)
        {
            try
            {
                var tableColumnsContext = Unsafe.AsRef<TableColumnsContext>((void*)tableColumnsContextPtr);

                var column = new TableColumn
                {
                    // From `CqlColumn`
                    Index = -1, // FIXME
                    // Type stays null, like in the legacy driver. See the remarks of TableColumn.
                    // From `ColumnDesc`
                    Name = columnName.ToManagedString(),
                    TypeCode = (ColumnTypeCode)typeCode,
                    TypeInfo = typeInfoPtr != IntPtr.Zero ? BridgedRowSet.BuildTypeInfoFromHandle(typeInfoPtr, (ColumnTypeCode)typeCode, tableColumnsContext.KeyspaceName) : null,
                    IsStatic = isStatic,
                    IsFrozen = isFrozen
                };
                tableColumnsContext.Columns.Add(column);

                return FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIMaybeException> AddPrimaryKeyPtr = &AddPrimaryKey;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException AddPrimaryKey(
            IntPtr keysListPtr,
            FFIString keyName)
        {
            try
            {
                var keysList = Unsafe.AsRef<List<string>>((void*)keysListPtr);
                keysList.Add(keyName.ToManagedString());
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException cluster_state_get_table_metadata(
            IntPtr clusterState,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspaceName,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string tableName,
            IntPtr tableColumnsContextPtr,
            IntPtr constructTableColumnCallback,
            IntPtr partitionKeysListPtr,
            IntPtr clusteringKeysListPtr,
            IntPtr AddPrimaryKeyCallback,
            IntPtr tableContextPtr,
            IntPtr constructTableMetadataCallback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, IntPtr, IntPtr, IntPtr, FFIMaybeException> FillTableMetadataPtr = &FillTableMetadata;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException FillTableMetadata(
            IntPtr tableContextPtr,
            IntPtr tableColumnsContextPtr,
            IntPtr partitionKeys,
            IntPtr clusteringKeys)
        {
            try
            {
                var tableMetadata = Unsafe.AsRef<TableMetadata>((void*)tableContextPtr);
                var tableColumnsContext = Unsafe.AsRef<TableColumnsContext>((void*)tableColumnsContextPtr);
                var tableColumnsList = tableColumnsContext.Columns;

                // Rust keeps the columns in a HashMap, so their order is not stable.
                // Legacy read them from system_schema.columns, which is clustered by column_name,
                // so TableColumns was sorted by name. Keep that order.
                // Sort in place: no LINQ buffers, and the static lambda is cached.
                tableColumnsList.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));
                var tableColumnsDictionary = new Dictionary<string, TableColumn>();
                foreach (var tc in tableColumnsList)
                {
                    tc.Keyspace = tableMetadata.KeyspaceName;
                    tc.Table = tableMetadata.Name;
                    tableColumnsDictionary[tc.Name] = tc;
                }

                var partitionKeysList = Unsafe.AsRef<List<string>>((void*)partitionKeys);
                TableColumn[] partitionKeysColumns = new TableColumn[partitionKeysList.Count];
                for (int i = 0; i < partitionKeysList.Count; i++)
                {
                    var pkName = partitionKeysList[i];
                    if (!tableColumnsDictionary.TryGetValue(pkName, out var column))
                    {
                        throw new InvalidOperationException($"Partition key column '{pkName}' not found in columns list for table '{tableMetadata.Name}'.");
                    }
                    column.KeyType = KeyType.Partition;
                    partitionKeysColumns[i] = column;
                }

                // FIXME: we currently don't have access to clustering key order info, so we default to Ascending.
                var clusteringKeysList = Unsafe.AsRef<List<string>>((void*)clusteringKeys);
                var clusteringKeysColumns = new Tuple<TableColumn, DataCollectionMetadata.SortOrder>[clusteringKeysList.Count];
                for (int i = 0; i < clusteringKeysList.Count; i++)
                {
                    var ckName = clusteringKeysList[i];
                    if (!tableColumnsDictionary.TryGetValue(ckName, out var column))
                    {
                        throw new InvalidOperationException($"Clustering key column '{ckName}' not found in columns list for table '{tableMetadata.Name}'.");
                    }
                    column.KeyType = KeyType.Clustering;
                    clusteringKeysColumns[i] = new Tuple<TableColumn, DataCollectionMetadata.SortOrder>(column, DataCollectionMetadata.SortOrder.Ascending);
                }

                // TODO: bridge table options.

                // TableColumns is a TableColumn[] in the public API, so copy the sorted list into an array.
                // To remove this copy, the FFI could fill an array directly. Rust would then have to
                // pass the column count separately, before it adds the columns.
                tableMetadata.SetValues(tableColumnsDictionary, tableColumnsList.ToArray(), partitionKeysColumns, clusteringKeysColumns, null);

                return FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }
        }

        internal TableMetadata GetTableMetadata(string keyspaceName, string tableName)
        {
            var tmdSlot = new StackSlot<TableMetadata>(new(keyspaceName, tableName));
            var tmdPtr = tmdSlot.AddressOf();
            var tableColumnsContextSlot = new StackSlot<TableColumnsContext>(new(keyspaceName));
            var tableColumnsContextPtr = tableColumnsContextSlot.AddressOf();
            var partitionKeysSlot = new StackSlot<List<string>>(new());
            var partitionKeysPtr = partitionKeysSlot.AddressOf();
            var clusteringKeysSlot = new StackSlot<List<string>>(new());
            var clusteringKeysPtr = clusteringKeysSlot.AddressOf();

            try
            {
                unsafe
                {
                    RunWithIncrement(handle =>
                        cluster_state_get_table_metadata(
                            handle,
                            keyspaceName,
                            tableName,
                            tableColumnsContextPtr,
                            (IntPtr)ConstructTableColumnPtr,
                            partitionKeysPtr,
                            clusteringKeysPtr,
                            (IntPtr)AddPrimaryKeyPtr,
                            tmdPtr,
                            (IntPtr)FillTableMetadataPtr,
                            (IntPtr)Globals.ConstructorsPtr
                        )
                    );
                }
            }
            catch (InvalidArgumentException)
            {
                // If the keyspace or table was not found return null.
                return null;
            }
            catch (Exception ex)
            {
                // For other exceptions, rethrow as they indicate a failure in metadata retrieval rather than missing table.
                throw new InvalidOperationException($"Error retrieving metadata for table '{tableName}' in keyspace '{keyspaceName}'.", ex);
            }

            return tmdSlot.Value;
        }
    }
}
