//
//      Copyright (C) ScyllaDB
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
using Cassandra.Serialization;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.Tests
{
    /// <summary>
    /// <see cref="CqlColumn.Type"/> must be the CLR type that the serializer produces.
    /// The Mapper and <see cref="Row.GetEnumerator"/> read values as this type.
    /// Legacy set it with <c>serializer.GetClrType(typeCode, typeInfo)</c>.
    /// </summary>
    [TestFixture]
    public class CqlColumnTypeTests
    {
        private class Address
        {
            public string Street { get; set; }
        }

        private static CqlColumn Column(ColumnTypeCode typeCode, IColumnInfo typeInfo = null)
        {
            return new CqlColumn { Name = typeCode.ToString(), TypeCode = typeCode, TypeInfo = typeInfo };
        }

        private static Type ClrTypeOf(IGenericSerializer serializer, CqlColumn column)
        {
            var columns = new[] { column };
            BridgedRowSet.AssignClrTypes(columns, serializer);
            return columns[0].Type;
        }

        [Test]
        public void AssignClrTypes_List_Is_IEnumerable_Of_Value_Type()
        {
            var column = Column(ColumnTypeCode.List, new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Text });
            Assert.AreEqual(typeof(IEnumerable<string>), ClrTypeOf(new GenericSerializer(), column));
        }

        [Test]
        public void AssignClrTypes_Set_Is_IEnumerable_Of_Key_Type()
        {
            var column = Column(ColumnTypeCode.Set, new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Int });
            Assert.AreEqual(typeof(IEnumerable<int>), ClrTypeOf(new GenericSerializer(), column));
        }

        [Test]
        public void AssignClrTypes_Map_Is_IDictionary_Of_Key_And_Value_Types()
        {
            var column = Column(ColumnTypeCode.Map, new MapColumnInfo
            {
                KeyTypeCode = ColumnTypeCode.Text,
                ValueTypeCode = ColumnTypeCode.Bigint
            });
            Assert.AreEqual(typeof(IDictionary<string, long>), ClrTypeOf(new GenericSerializer(), column));
        }

        [Test]
        public void AssignClrTypes_Timestamp_Is_DateTimeOffset()
        {
            Assert.AreEqual(typeof(DateTimeOffset), ClrTypeOf(new GenericSerializer(), Column(ColumnTypeCode.Timestamp)));
        }

        [Test]
        public void AssignClrTypes_Mapped_Udt_Is_The_Mapped_Poco_Type()
        {
            var serializer = new GenericSerializer();
            serializer.SetUdtMap("ks.address", UdtMap.For<Address>("address", "ks"));
            var column = Column(ColumnTypeCode.Udt, new UdtColumnInfo("ks.address"));
            Assert.AreEqual(typeof(Address), ClrTypeOf(serializer, column));
        }

        [Test]
        public void AssignClrTypes_Unmapped_Udt_Is_Byte_Array()
        {
            var column = Column(ColumnTypeCode.Udt, new UdtColumnInfo("ks.unmapped"));
            Assert.AreEqual(typeof(byte[]), ClrTypeOf(new GenericSerializer(), column));
        }

        [Test]
        public void AssignClrTypes_Custom_Without_TypeInfo_Is_Byte_Array()
        {
            // Rust reports an unknown type as Custom (0x00) with no type info. Must not throw.
            Assert.AreEqual(typeof(byte[]), ClrTypeOf(new GenericSerializer(), Column(ColumnTypeCode.Custom)));
        }

        [Test]
        public void AssignClrTypes_Sets_Every_Column()
        {
            var columns = new[]
            {
                Column(ColumnTypeCode.Int),
                Column(ColumnTypeCode.Varchar),
                Column(ColumnTypeCode.Uuid),
                Column(ColumnTypeCode.Vector, new VectorColumnInfo { ValueTypeCode = ColumnTypeCode.Float, Dimensions = 3 }),
            };
            BridgedRowSet.AssignClrTypes(columns, new GenericSerializer());
            Assert.AreEqual(typeof(int), columns[0].Type);
            Assert.AreEqual(typeof(string), columns[1].Type);
            Assert.AreEqual(typeof(Guid), columns[2].Type);
            Assert.AreEqual(typeof(CqlVector<float>), columns[3].Type);
        }
    }
}
