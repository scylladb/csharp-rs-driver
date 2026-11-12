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

        private static IPrimeRequest LocalSchemaVersionQueryPrime(Guid version) =>
            SimulacronBase
                .PrimeBuilder()
                .WhenQuery(SchemaAgreementSimulacronTests.LocalSchemaVersionQuery)
                .ThenRowsSuccess(new[] { ("schema_version", DataType.Uuid) }, rows => rows.WithRow(version))
                .BuildRequest();

        private static IPrimeRequest PeersSchemaVersionQueryPrime(IEnumerable<Guid> versions) =>
            SimulacronBase
                .PrimeBuilder()
                .WhenQuery(SchemaAgreementSimulacronTests.PeersSchemaVersionQuery)
                .ThenRowsSuccess(
                    new[] { ("schema_version", DataType.Uuid) },
                    rows => rows.WithRows(versions.Select(v => new object[] { v }).ToArray()))
                .BuildRequest();

        private Cluster BuildCluster(SimulacronCluster simulacronCluster)
        {
            return ClusterBuilder()
                          .AddContactPoint(simulacronCluster.InitialContactPoint)
                          .WithSocketOptions(
                              new SocketOptions()
                                  .SetReadTimeoutMillis(5000)
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
    }
}