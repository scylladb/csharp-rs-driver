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

namespace Cassandra
{
    /// <summary>
    /// A variable on the stack that holds a value for the duration of a synchronous FFI call.
    /// Get its address with <c>slot.AddressOf()</c>, see <see cref="StackSlot.AddressOf{T}(ref StackSlot{T})"/>.
    ///
    /// Goal: costless stack pinning, i.e., obtaining a stable address to reach a heap-allocated object, without need
    /// of employing dynamic GCHandle-based techniques.
    /// </summary>
    /// <remarks>
    /// <para>
    /// An FFI call gives Rust the address of a stack variable that holds a heap-allocated C# value.
    /// Rust gives this address back to a C# callback.
    /// The callback reads the variable with <c>Unsafe.AsRef&lt;T&gt;((void*)address)</c>,
    /// thus reaching the heap-allocated C# value.
    /// </para>
    /// <para>
    /// The address stays valid only if the variable is on the stack:
    /// <list type="bullet">
    /// <item>A stack variable does not move. When the GC moves the object, it updates the reference in the variable.</item>
    /// <item>A variable on the heap moves with the object that contains it. After the move, the GC does not update
    /// an address that native code holds. After a GC, the address points to memory that can hold other data!</item>
    /// </list>
    /// </para>
    /// <para>
    /// Problem: the C# compiler moves a local variable to the heap, with no warning, in these cases:
    /// <list type="bullet">
    /// <item>A lambda or a local function uses the variable. The variable goes into a closure object.</item>
    /// <item>An async method uses the variable across an <c>await</c>. The variable goes into the state machine.</item>
    /// <item>An iterator uses the variable across a <c>yield</c>.</item>
    /// </list>
    /// Solution: a <c>ref struct</c> cannot be on the heap. The compiler gives an error in each of these cases.
    /// Thus a <see cref="StackSlot{T}"/> is always on the stack.
    /// The variable that you copy the value from into the StackSlot can be anywhere.
    /// </para>
    /// <para>
    /// Use this pattern:
    /// <code>
    /// var contextSlot = new StackSlot&lt;GetReplicasContext&gt;(new(hostsById));
    /// var contextPtr = contextSlot.AddressOf();
    /// RunWithIncrement(handle =&gt; ffi_call(handle, contextPtr));
    /// return contextSlot.Value.Replicas;
    /// </code>
    /// Create the value inside the slot. Do not keep the value in a second local variable.
    /// After the call, read the value from <see cref="Value"/>.
    /// Then each value has only two names: the slot and its address.
    /// </para>
    /// <para>
    /// The compiler does not check these rules:
    /// <list type="bullet">
    /// <item>Use the address only during the FFI call. Rust must not keep the address after the call returns.</item>
    /// <item>Do not return the address from the method that holds the slot.</item>
    /// <item>The slot holds a copy of the value. If a callback writes a new value through the address,
    /// read the result from <see cref="Value"/>. The variable that you copied the value from does not change.</item>
    /// </list>
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The type of the value. A reference type or a struct.</typeparam>
    internal ref struct StackSlot<T>
    {
        /// <summary>
        /// The value that the callbacks read, and can write, through the stable slot address.
        /// </summary>
        internal T Value;

        internal StackSlot(T value)
        {
            Value = value;
        }
    }

    /// <summary>
    /// Operations on <see cref="StackSlot{T}"/>.
    /// </summary>
    internal static class StackSlot
    {
        /// <summary>
        /// Gives the address of the value in <paramref name="slot"/>, to pass to an FFI call.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is an extension method with a <c>this ref</c> receiver. Call it as <c>slot.AddressOf()</c>.
        /// </para>
        /// <para>
        /// The receiver is <c>ref</c>, thus the caller must give a named local variable.
        /// The compiler rejects a temporary value, such as <c>new StackSlot&lt;T&gt;(x).AddressOf()</c> (CS1510),
        /// because it can use the stack location of a temporary value again after the statement,
        /// which would mean a dangling pointer.
        /// The compiler also rejects a read-only variable, such as an <c>in</c> parameter (CS8329).
        /// </para>
        /// <para>
        /// Do not change this method to an instance method of <see cref="StackSlot{T}"/>.
        /// The compiler accepts an instance call on a temporary value.
        /// On a read-only variable, the compiler calls an instance method on a hidden copy.
        /// The address then points to the copy.
        /// </para>
        /// </remarks>
        internal static unsafe IntPtr AddressOf<T>(this ref StackSlot<T> slot)
        {
            return (IntPtr)Unsafe.AsPointer(ref slot.Value);
        }
    }
}
