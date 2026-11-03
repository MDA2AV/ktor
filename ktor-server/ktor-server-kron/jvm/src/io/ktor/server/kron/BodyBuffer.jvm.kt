/*
 * Copyright 2014-2025 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.server.kron

import kotlinx.io.Buffer

// The JVM's kotlinx-io pools its segments: a write into an empty buffer reuses one
internal actual fun Buffer.append(bytes: ByteArray, from: Int, to: Int) {
    write(bytes, from, to)
}
