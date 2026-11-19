//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class SchemaAgreementSimulacronTests : TestGlobals
    {
        private SimulacronCluster _simulacronCluster;
        private Cluster _cluster;
        private const int MaxSchemaAgreementWaitSeconds = 10;

        private const string LocalSchemaVersionQuery = "SELECT schema_version FROM system.local WHERE key='local'";
        private const string PeersSchemaVersionQuery = "SELECT schema_version FROM system.peers";

        private static IPrimeRequest LocalSchemaVersionQueryPrime(Guid version, int delayInMs = 0) =>
            SimulacronBase
                .PrimeBuilder()
                .WhenQuery(SchemaAgreementSimulacronTests.LocalSchemaVersionQuery)
                .ThenRowsSuccess(new[] { ("schema_version", DataType.Uuid) }, rows => rows.WithRow(version))
                .WithDelayInMs(delayInMs)
                .BuildRequest();

        private static IPrimeRequest PeersSchemaVersionQueryPrime(IEnumerable<Guid> versions) =>
            SimulacronBase
                .PrimeBuilder()
                .WhenQuery(SchemaAgreementSimulacronTests.PeersSchemaVersionQuery)
                .ThenRowsSuccess(
                    new[] { ("schema_version", DataType.Uuid) },
                    rows => rows.WithRows(versions.Select(v => new object[] { v }).ToArray()))
                .BuildRequest();

        private Cluster BuildCluster(SimulacronCluster simulacronCluster, int readTimeoutMillis = 5000)
        {
            return ClusterBuilder()
                          .AddContactPoint(simulacronCluster.InitialContactPoint)
                          .WithSocketOptions(
                              new SocketOptions()
                                  .SetReadTimeoutMillis(readTimeoutMillis)
                                  .SetConnectTimeoutMillis(10000))
                          .WithMaxSchemaAgreementWaitSeconds(SchemaAgreementSimulacronTests.MaxSchemaAgreementWaitSeconds)
                          .Build();
        }

        [TearDown]
        public void TearDown()
        {
            _simulacronCluster?.Dispose();
            _simulacronCluster = null;
            _cluster?.Dispose();
            _cluster = null;
        }

        [Test]
        public async Task Should_CheckSchemaAgreementReturnTrue_When_OneSchemaVersionIsReturnedByQueries()
        {
            var schemaVersion = Guid.NewGuid();
            _simulacronCluster = await SimulacronCluster.CreateNewAsync(3).ConfigureAwait(false);

            _simulacronCluster.Prime(SchemaAgreementSimulacronTests.LocalSchemaVersionQueryPrime(schemaVersion));
            _simulacronCluster.Prime(SchemaAgreementSimulacronTests.PeersSchemaVersionQueryPrime(Enumerable.Repeat(schemaVersion, 2)));
            _cluster = BuildCluster(_simulacronCluster);

            _cluster.Connect();
            Assert.IsTrue(await _cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false));
        }

        [Test]
        public async Task Should_CheckSchemaAgreementReturnFalse_When_SchemaVersionQueryTimesOut()
        {
            const int readTimeoutMillis = 1000;
            // Without the timeout, the check would wait for the delayed answer and return true.
            const int delayInMs = 30000;
            const int maxElapsedMillis = 10000;

            _simulacronCluster = await SimulacronCluster.CreateNewAsync(3).ConfigureAwait(false);
            _simulacronCluster.Prime(SchemaAgreementSimulacronTests.LocalSchemaVersionQueryPrime(Guid.NewGuid(), delayInMs));
            _cluster = BuildCluster(_simulacronCluster, readTimeoutMillis);
            var session = (Session)_cluster.Connect();

            // The public method returns false for all errors, so first check the cause with the internal method.
            var stopwatch = Stopwatch.StartNew();
            Assert.ThrowsAsync<SchemaAgreementTimeoutException>(() => session.CheckSchemaAgreementAsync(readTimeoutMillis));
            AssertElapsed(stopwatch, readTimeoutMillis, maxElapsedMillis);

            // Like the legacy driver: the public method uses the read timeout and returns false.
            stopwatch.Restart();
            Assert.IsFalse(await _cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false));
            AssertElapsed(stopwatch, readTimeoutMillis, maxElapsedMillis);

            // The checks released the session read lock, so the shutdown does not wait for the delayed answers.
            stopwatch.Restart();
            await _cluster.ShutdownAsync().ConfigureAwait(false);
            Assert.Less(stopwatch.ElapsedMilliseconds, maxElapsedMillis, "The shutdown waited for the schema agreement check");
        }

        private static void AssertElapsed(Stopwatch stopwatch, int timeoutMillis, int maxElapsedMillis)
        {
            // Allow a small error, because the timer and the stopwatch are not exact.
            Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, timeoutMillis - 100, "The check ended before the timeout");
            Assert.Less(stopwatch.ElapsedMilliseconds, maxElapsedMillis, "The check did not stop at the timeout");
        }
    }
}