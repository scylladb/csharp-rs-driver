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
using System.Runtime.CompilerServices;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.Tests
{
    /// <summary>
    /// An FFI call gives Rust the address of a C# variable. Rust gives the address back to C# callbacks.
    /// Between two callbacks, the GC can move objects.
    /// These tests move objects with the GC while the address is in use, and then use the memory again.
    /// They do what a production FFI call does when a GC occurs between two callbacks.
    /// </summary>
    [TestFixture]
    public class StackSlotTests
    {
        private sealed class Context
        {
            internal readonly string Name;

            internal Context(string name)
            {
                Name = name;
            }
        }

        /// <summary>
        /// A struct with references, such as <see cref="PopulateValuesContext"/>.
        /// </summary>
        private struct ContextPair
        {
            internal Context First;
            internal Context Second;
        }

        /// <summary>
        /// Puts dead objects before the objects of the test.
        /// A compacting GC then moves the objects of the test into the free space.
        /// </summary>
        [SetUp]
        public void AllocateGarbage()
        {
            for (var i = 0; i < 10_000; i++)
            {
                GC.KeepAlive(new byte[64]);
            }
        }

        /// <summary>
        /// Moves the young objects with a compacting GC.
        /// Then allocates new objects in the memory that the GC made free.
        /// After this, an address into the old location of a moved object points to other data.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void MoveObjectsAndReuseMemory()
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            var fill = new object[20_000];
            for (var i = 0; i < fill.Length; i++)
            {
                fill[i] = new long[] { 0x4141414141414141, 0x4141414141414141 };
            }
            GC.KeepAlive(fill);
        }

        /// <summary>
        /// Gives the current location of the object that <paramref name="variable"/> refers to.
        /// The tests use it to check that the GC moved the object.
        /// The variable must be a local that no lambda uses, so that it is on the stack.
        /// </summary>
        private static IntPtr LocationOf<T>(ref T variable) where T : class
        {
            return Unsafe.As<T, IntPtr>(ref variable);
        }

        /// <summary>
        /// Acts as a callback that Rust calls with the slot address, after a GC.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static unsafe Context ReadAfterGc(IntPtr address)
        {
            MoveObjectsAndReuseMemory();
            return Unsafe.AsRef<Context>((void*)address);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static unsafe ContextPair ReadPairAfterGc(IntPtr address)
        {
            MoveObjectsAndReuseMemory();
            return Unsafe.AsRef<ContextPair>((void*)address);
        }

        /// <summary>
        /// Acts as a callback that writes a new object into the slot, such as <c>BridgedRowSet.SetCoordinator</c>.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static unsafe Context WriteAfterGc(IntPtr address)
        {
            MoveObjectsAndReuseMemory();
            var written = new Context("written by the callback");
            Unsafe.AsRef<Context>((void*)address) = written;
            return written;
        }

        /// <summary>
        /// Shows the problem that <see cref="StackSlot{T}"/> prevents.
        /// The lambda uses <c>context</c>, thus the compiler puts <c>context</c> into a closure object on the heap.
        /// When the GC moves the closure, the address of <c>context</c> changes.
        /// A callback that holds the old address then reads other data.
        /// This test only compares the addresses. It does not read through the old address,
        /// because that memory now holds other objects.
        /// </summary>
        [Test]
        public void AddressOfCapturedLocal_Changes_When_GcMovesTheClosure()
        {
            var context = new Context("table columns");
            var addressBeforeGc = IntPtr.Zero;
            var addressAfterGc = IntPtr.Zero;

            Action ffiCall = () =>
            {
                unsafe
                {
                    addressBeforeGc = (IntPtr)Unsafe.AsPointer(ref context);
                    MoveObjectsAndReuseMemory();
                    addressAfterGc = (IntPtr)Unsafe.AsPointer(ref context);
                }
            };
            ffiCall();

            Assert.AreNotEqual(addressBeforeGc, addressAfterGc);
        }

        [Test]
        public void AddressOf_GivesTheSameObject_When_GcMovesTheObject()
        {
            var context = new Context("table columns");
            var slot = new StackSlot<Context>(context);
            var address = slot.AddressOf();
            var locationBeforeGc = LocationOf(ref context);

            var read = ReadAfterGc(address);

            // If the GC did not move the object, this test proves nothing.
            Assume.That(LocationOf(ref context), Is.Not.EqualTo(locationBeforeGc), "The GC did not move the object.");
            Assert.AreSame(context, read);
            Assert.AreEqual("table columns", read.Name);
        }

        [Test]
        public void AddressOf_GivesTheSameReferences_When_SlotHoldsAStruct()
        {
            var first = new Context("first");
            var second = new Context("second");
            var slot = new StackSlot<ContextPair>(new ContextPair { First = first, Second = second });
            var address = slot.AddressOf();
            var locationBeforeGc = LocationOf(ref first);

            var read = ReadPairAfterGc(address);

            Assume.That(LocationOf(ref first), Is.Not.EqualTo(locationBeforeGc), "The GC did not move the object.");
            Assert.AreSame(first, read.First);
            Assert.AreSame(second, read.Second);
        }

        [Test]
        public void Value_HoldsTheNewObject_When_CallbackWritesThroughTheAddress()
        {
            Context source = null;
            var slot = new StackSlot<Context>(source);
            var address = slot.AddressOf();

            var written = WriteAfterGc(address);

            Assert.AreSame(written, slot.Value);
            // The slot holds a copy. The variable that the value came from does not change.
            Assert.IsNull(source);
        }
    }
}
