import { createHasher } from './hash.ts'
import { openWritable, type ScannedFile } from './fs.ts'
import { saveSession } from './idb.ts'
import {
  BUFFER_HIGH,
  BUFFER_LOW,
  CHUNK_SIZE,
  READ_AHEAD,
  WRITE_BACKPRESSURE,
  type ControlMessage,
  type Manifest,
  type TransferProgress,
} from './types.ts'

export class TransferGate {
  paused = false
  cancelled = false
  private waiters: Array<() => void> = []

  pause(): void {
    this.paused = true
  }

  resume(): void {
    this.paused = false
    const pending = this.waiters.splice(0)
    for (const resume of pending) resume()
  }

  cancel(): void {
    this.cancelled = true
    this.resume()
  }

  async waitIfPaused(): Promise<void> {
    while (this.paused && !this.cancelled) {
      await new Promise<void>((resolve) => this.waiters.push(resolve))
    }
    if (this.cancelled) throw new Error('Transfer cancelled')
  }
}

type InboxEvent =
  | { kind: 'control'; message: ControlMessage }
  | { kind: 'data'; buffer: ArrayBuffer }
  | { kind: 'close' }

export class ChannelInbox {
  private queue: InboxEvent[] = []
  private waiters: Array<() => void> = []
  private closed = false

  constructor(channel: RTCDataChannel) {
    channel.addEventListener('message', (event) => {
      if (typeof event.data === 'string') {
        try {
          this.push({ kind: 'control', message: JSON.parse(event.data) as ControlMessage })
        } catch {
          // ignore
        }
        return
      }
      if (event.data instanceof ArrayBuffer) {
        this.push({ kind: 'data', buffer: event.data })
        return
      }
      void (event.data as Blob).arrayBuffer().then((buffer) => {
        this.push({ kind: 'data', buffer })
      })
    })
    channel.addEventListener('close', () => {
      this.closed = true
      this.push({ kind: 'close' })
    })
  }

  private push(event: InboxEvent): void {
    this.queue.push(event)
    const waiters = this.waiters.splice(0)
    for (const waiter of waiters) waiter()
  }

  async next(): Promise<InboxEvent> {
    while (this.queue.length === 0) {
      if (this.closed) return { kind: 'close' }
      await new Promise<void>((resolve) => this.waiters.push(resolve))
    }
    return this.queue.shift()!
  }
}

export function sendControl(channel: RTCDataChannel, message: ControlMessage): void {
  if (channel.readyState !== 'open') throw new Error('Connection closed')
  channel.send(JSON.stringify(message))
}

export function listenRemoteGate(channel: RTCDataChannel, gate: TransferGate): void {
  channel.addEventListener('message', (event) => {
    if (typeof event.data !== 'string') return
    try {
      const message = JSON.parse(event.data) as ControlMessage
      if (message.type === 'pause') gate.pause()
      if (message.type === 'resume') gate.resume()
      if (message.type === 'cancel') gate.cancel()
    } catch {
      // ignore non-control text
    }
  })
}

function pickChannel(channels: RTCDataChannel[]): RTCDataChannel {
  let best = channels[0]
  for (const channel of channels) {
    if (channel.readyState === 'open' && channel.bufferedAmount < best.bufferedAmount) {
      best = channel
    }
  }
  return best
}

function encodeChunk(offset: number, payload: ArrayBuffer): ArrayBuffer {
  const out = new ArrayBuffer(8 + payload.byteLength)
  new DataView(out).setBigUint64(0, BigInt(offset), false)
  new Uint8Array(out, 8).set(new Uint8Array(payload))
  return out
}

function decodeChunk(buffer: ArrayBuffer): { offset: number; payload: Uint8Array } {
  const view = new DataView(buffer)
  return {
    offset: Number(view.getBigUint64(0, false)),
    payload: new Uint8Array(buffer, 8),
  }
}

async function waitForBuffer(channel: RTCDataChannel): Promise<void> {
  if (channel.readyState !== 'open') throw new Error('Connection closed')
  if (channel.bufferedAmount <= BUFFER_HIGH) return
  await new Promise<void>((resolve, reject) => {
    const done = () => {
      channel.removeEventListener('bufferedamountlow', done)
      channel.removeEventListener('close', onClose)
      resolve()
    }
    const onClose = () => {
      channel.removeEventListener('bufferedamountlow', done)
      channel.removeEventListener('close', onClose)
      reject(new Error('Connection closed'))
    }
    channel.bufferedAmountLowThreshold = BUFFER_LOW
    channel.addEventListener('bufferedamountlow', done)
    channel.addEventListener('close', onClose)
  })
}

async function waitUntilDrained(channels: RTCDataChannel[]): Promise<void> {
  await Promise.all(
    channels.map(
      (channel) =>
        new Promise<void>((resolve, reject) => {
          if (channel.readyState !== 'open') {
            reject(new Error('Connection closed'))
            return
          }
          if (channel.bufferedAmount === 0) {
            resolve()
            return
          }
          const done = () => {
            if (channel.bufferedAmount > 0) return
            channel.removeEventListener('bufferedamountlow', done)
            channel.removeEventListener('close', onClose)
            resolve()
          }
          const onClose = () => {
            channel.removeEventListener('bufferedamountlow', done)
            channel.removeEventListener('close', onClose)
            reject(new Error('Connection closed'))
          }
          channel.bufferedAmountLowThreshold = 0
          channel.addEventListener('bufferedamountlow', done)
          channel.addEventListener('close', onClose)
        }),
    ),
  )
}

class Speedometer {
  private samples: Array<{ t: number; bytes: number }> = []

  push(bytes: number): number {
    const now = performance.now()
    this.samples.push({ t: now, bytes })
    const cutoff = now - 2000
    this.samples = this.samples.filter((s) => s.t >= cutoff)
    if (this.samples.length < 2) return 0
    const first = this.samples[0]
    const last = this.samples[this.samples.length - 1]
    const dt = (last.t - first.t) / 1000
    if (dt <= 0) return 0
    const total = this.samples.slice(1).reduce((sum, s) => sum + s.bytes, 0)
    return total / dt
  }
}

function throttleProgress(onProgress: (progress: TransferProgress) => void) {
  let last = 0
  let pending: TransferProgress | null = null
  let timer = 0
  const flush = () => {
    timer = 0
    if (!pending) return
    onProgress(pending)
    pending = null
    last = performance.now()
  }
  return {
    push(progress: TransferProgress, force = false) {
      pending = progress
      const now = performance.now()
      if (force || now - last >= 120) {
        if (timer) window.clearTimeout(timer)
        flush()
        return
      }
      if (!timer) timer = window.setTimeout(flush, 120)
    },
  }
}

export async function sendFolder(options: {
  channel: RTCDataChannel
  dataChannels: RTCDataChannel[]
  inbox: ChannelInbox
  roomCode: string
  files: ScannedFile[]
  manifest: Manifest
  gate: TransferGate
  onProgress: (progress: TransferProgress) => void
}): Promise<void> {
  const { channel, inbox, roomCode, files, manifest, gate, onProgress } = options
  const lanes =
    manifest.verifyChecksums || options.dataChannels.length === 0
      ? [options.dataChannels[0] ?? channel]
      : options.dataChannels
  listenRemoteGate(channel, gate)
  const speed = new Speedometer()
  const progress = throttleProgress(onProgress)

  let readyResume: { index: number; offset: number } | undefined
  const started = Date.now()
  while (!readyResume) {
    if (Date.now() - started > 15000) throw new Error('Receiver did not become ready')
    const event = await inbox.next()
    if (event.kind === 'control' && event.message.type === 'ready') {
      readyResume = event.message.resumeFrom ?? { index: 0, offset: 0 }
      break
    }
    if (event.kind === 'close') throw new Error('Connection closed while waiting for the receiver')
  }

  sendControl(channel, { type: 'manifest', manifest })

  let bytesDone = 0
  for (let i = 0; i < readyResume.index; i += 1) bytesDone += files[i]?.size ?? 0
  bytesDone += readyResume.offset
  let lastSave = 0

  for (let index = readyResume.index; index < files.length; index += 1) {
    await gate.waitIfPaused()
    const file = files[index]
    const offset = index === readyResume.index ? readyResume.offset : 0
    sendControl(channel, { type: 'file-start', index, path: file.path, size: file.size, offset })

    const blob = await file.handle.getFile()
    const hasher = manifest.verifyChecksums ? await createHasher() : null
    let sent = offset

    if (offset > 0 && hasher) {
      const prefix = blob.slice(0, offset)
      const reader = prefix.stream().getReader()
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        if (value) hasher.update(value)
      }
    }

    let pos = offset
    let nextRead = pos < blob.size ? blob.slice(pos, Math.min(pos + READ_AHEAD, blob.size)).arrayBuffer() : null

    while (nextRead) {
      await gate.waitIfPaused()
      const block = await nextRead
      pos += block.byteLength
      nextRead = pos < blob.size ? blob.slice(pos, Math.min(pos + READ_AHEAD, blob.size)).arrayBuffer() : null
      hasher?.update(new Uint8Array(block))

      for (let inner = 0; inner < block.byteLength; inner += CHUNK_SIZE) {
        await gate.waitIfPaused()
        const lane = pickChannel(lanes)
        await waitForBuffer(lane)
        const piece = block.slice(inner, Math.min(inner + CHUNK_SIZE, block.byteLength))
        const fileOffset = sent
        lane.send(encodeChunk(fileOffset, piece))
        sent += piece.byteLength
        bytesDone += piece.byteLength
        progress.push({
          fileIndex: index,
          filePath: file.path,
          fileBytes: sent,
          fileSize: file.size,
          bytesDone,
          totalBytes: manifest.totalBytes,
          speed: speed.push(piece.byteLength),
        })
      }
    }

    await waitUntilDrained(lanes)
    sendControl(channel, { type: 'file-end', index, sha256: hasher ? await hasher.digest() : undefined })
    progress.push(
      {
        fileIndex: index,
        filePath: file.path,
        fileBytes: sent,
        fileSize: file.size,
        bytesDone,
        totalBytes: manifest.totalBytes,
        speed: speed.push(1),
      },
      true,
    )

    if (Date.now() - lastSave > 4000 || index === files.length - 1) {
      lastSave = Date.now()
      await saveSession({
        roomCode,
        role: 'sender',
        manifest,
        fileIndex: index + 1,
        fileOffset: 0,
        bytesDone,
        verifyChecksums: manifest.verifyChecksums,
        updatedAt: Date.now(),
      })
    }
  }

  sendControl(channel, { type: 'done' })
}

export async function receiveFolder(options: {
  channel: RTCDataChannel
  dataChannels: RTCDataChannel[]
  inbox: ChannelInbox
  roomCode: string
  dest: FileSystemDirectoryHandle
  gate: TransferGate
  resumeFrom?: { index: number; offset: number }
  onManifest: (manifest: Manifest) => void
  onProgress: (progress: TransferProgress) => void
}): Promise<void> {
  const { channel, inbox, roomCode, dest, gate, onManifest, onProgress } = options
  const lanes = options.dataChannels.length > 0 ? options.dataChannels : [channel]
  const speed = new Speedometer()
  const progress = throttleProgress(onProgress)

  sendControl(channel, { type: 'ready', resumeFrom: options.resumeFrom })
  const retry = window.setInterval(() => {
    try {
      sendControl(channel, { type: 'ready', resumeFrom: options.resumeFrom })
    } catch {
      window.clearInterval(retry)
    }
  }, 400)

  const io = {
    writable: null as FileSystemWritableFileStream | null,
    hasher: null as Awaited<ReturnType<typeof createHasher>> | null,
  }
  let manifest: Manifest | null = null
  let currentIndex = options.resumeFrom?.index ?? 0
  let currentPath = ''
  let currentSize = 0
  let received = options.resumeFrom?.offset ?? 0
  let bytesDone = 0
  let writeQueue = Promise.resolve()
  let ioLock = Promise.resolve()
  let outstanding = 0
  let lastSave = 0
  const earlyChunks: Array<{ offset: number; payload: Uint8Array }> = []

  const runExclusive = (fn: () => Promise<void>) => {
    const next = ioLock.then(fn)
    ioLock = next.catch(() => {})
    return next
  }

  const enqueueWrite = (offset: number, payload: Uint8Array) => {
    outstanding += payload.byteLength
    writeQueue = writeQueue.then(async () => {
      if (!io.writable) throw new Error('Received file data before file-start')
      await io.writable.write({ type: 'write', position: offset, data: payload.slice() })
      outstanding -= payload.byteLength
    })
  }

  const acceptChunk = async (buffer: ArrayBuffer) => {
    await gate.waitIfPaused()
    await runExclusive(async () => {
      const chunk = buffer.byteLength >= 8 ? decodeChunk(buffer) : { offset: received, payload: new Uint8Array(buffer) }
      if (!io.writable) {
        earlyChunks.push(chunk)
      } else {
        io.hasher?.update(chunk.payload)
        while (outstanding > WRITE_BACKPRESSURE) {
          await writeQueue
        }
        enqueueWrite(chunk.offset, chunk.payload)
      }
      received += chunk.payload.byteLength
      bytesDone += chunk.payload.byteLength
      if (manifest) {
        progress.push({
          fileIndex: currentIndex,
          filePath: currentPath,
          fileBytes: received,
          fileSize: currentSize,
          bytesDone,
          totalBytes: manifest.totalBytes,
          speed: speed.push(chunk.payload.byteLength),
        })
      }
    })
  }

  const onLaneMessage = (event: MessageEvent) => {
    const buffer =
      event.data instanceof ArrayBuffer
        ? event.data
        : null
    if (!buffer) {
      void (event.data as Blob).arrayBuffer().then((value) => {
        void acceptChunk(value)
      })
      return
    }
    void acceptChunk(buffer)
  }

  for (const lane of lanes) {
    lane.addEventListener('message', onLaneMessage)
  }

  try {
    while (true) {
      const event = await inbox.next()
      if (event.kind === 'close') throw new Error('Connection closed before the transfer finished')

      if (event.kind === 'control') {
        const msg = event.message
        if (msg.type === 'error') throw new Error(msg.message)
        if (msg.type === 'cancel') throw new Error(msg.reason ?? 'Sender cancelled')
        if (msg.type === 'pause') gate.pause()
        if (msg.type === 'resume') gate.resume()
        if (msg.type === 'manifest') {
          window.clearInterval(retry)
          manifest = msg.manifest
          onManifest(manifest)
          if (options.resumeFrom) {
            bytesDone = 0
            for (let i = 0; i < options.resumeFrom.index; i += 1) {
              bytesDone += manifest.files[i]?.size ?? 0
            }
            bytesDone += options.resumeFrom.offset
          }
        }
        if (msg.type === 'file-start') {
          await gate.waitIfPaused()
          await runExclusive(async () => {
            await writeQueue
            if (io.writable) {
              await io.writable.close()
              io.writable = null
            }
            if (!manifest) throw new Error('file-start arrived before the folder list')
            currentIndex = msg.index
            currentPath = msg.path
            currentSize = msg.size
            received = msg.offset
            io.hasher = manifest.verifyChecksums ? await createHasher() : null
            io.writable = await openWritable(dest, msg.path, msg.offset)
            writeQueue = Promise.resolve()
            outstanding = 0
            for (const chunk of earlyChunks.splice(0).sort((a, b) => a.offset - b.offset)) {
              io.hasher?.update(chunk.payload)
              enqueueWrite(chunk.offset, chunk.payload)
            }
          })
        }
        if (msg.type === 'file-end') {
          await writeQueue
          if (io.writable) {
            await io.writable.close()
            io.writable = null
          }
          if (manifest?.verifyChecksums && msg.sha256 && io.hasher) {
            const digest = await io.hasher.digest()
            if (digest !== msg.sha256) throw new Error(`Checksum mismatch for ${currentPath}`)
          }
          if (manifest && (Date.now() - lastSave > 4000 || currentIndex === manifest.files.length - 1)) {
            lastSave = Date.now()
            await saveSession({
              roomCode,
              role: 'receiver',
              manifest,
              fileIndex: currentIndex + 1,
              fileOffset: 0,
              bytesDone,
              verifyChecksums: manifest.verifyChecksums,
              updatedAt: Date.now(),
            })
          }
        }
        if (msg.type === 'done') {
          await writeQueue
          return
        }
        continue
      }

      await acceptChunk(event.buffer)
    }
  } finally {
    window.clearInterval(retry)
    for (const lane of lanes) {
      lane.removeEventListener('message', onLaneMessage)
    }
    if (io.writable) {
      try {
        await writeQueue
        await io.writable.close()
      } catch {
        // already closed
      }
    }
  }
}
