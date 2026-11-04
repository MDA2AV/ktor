/*
 * Copyright 2014-2025 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.server.kron

import kotlinx.io.Buffer
import kotlinx.io.UnsafeIoApi
import kotlinx.io.unsafe.UnsafeBufferOperations

// kotlinx-io has no segment pool on Native: a write into an empty buffer takes a new, zeroed 8 KiB segment, where
// an exact copy of a small body is all a request needs
@OptIn(UnsafeIoApi::class)
internal actual fun Buffer.append(bytes: ByteArray, from: Int, to: Int) {
    if (size == 0L && to - from in 1..EXACT_COPY_MAX) {
        UnsafeBufferOperations.moveToTail(this, bytes.copyOfRange(from, to))
    } else {
        write(bytes, from, to)
    }
}

private const val EXACT_COPY_MAX = 8192
