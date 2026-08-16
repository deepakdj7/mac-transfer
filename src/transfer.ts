import {
  addByteSpan,
  applyInventory,
  contiguousEnd,
  coveredBytes,
  destBytesFor,
  emptyChecklist,
  leftoverFiles,
  summarizeChecklist,
  type ByteSpan,
} from './checklist.ts'
import { createHasher, hashBlob } from './hash.ts'
import { getFileSize, inventoryFolder, openWritable, readDestFile, type ScannedFile } from './fs.ts'
import { saveSession } from './idb.ts'
import {
  BUFFER_HIGH,
  BUFFER_LOW,
  CHUNK_SIZE,
  READ_AHEAD,
  WRITE_BACKPRESSURE,
  type ControlMessage,
  type FileProgress,
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

export function wireLimit(pc?: RTCPeerConnection): number {
  const announced = pc?.sctp?.maxMessageSize
  const max = Number.isFinite(announced) && announced && announced > 0 ? announced : 65536
  return Math.max(8 * 1024, Math.min(48 * 1024, Math.floor(max) - 32))
}

class FileNackError extends Error {
  offset: number
  constructor(offset: number, reason: string) {
    super(reason)
    this.offset = offset
  }
}

function waitForFileControl(
  channel: RTCDataChannel,
  index: number,
  accept: ReadonlySet<ControlMessage['type']>,
): Promise<ControlMessage> {
  return new Promise((resolve, reject) => {
    const onMessage = (event: MessageEvent) => {
      if (typeof event.data !== 'string') return
      let message: ControlMessage
      try {
        message = JSON.parse(event.data) as ControlMessage
      } catch {
        return
      }
      if (message.type === 'file-nack' && message.index === index) {
        cleanup()
        reject(new FileNackError(message.offset, message.reason))
        return
      }
      if (accept.has(message.type) && 'index' in message && message.index === index) {
        cleanup()
        resolve(message)
        return
      }
      if (message.type === 'error') {
        cleanup()
        reject(new Error(message.message))
        return
      }
      if (message.type === 'cancel') {
        cleanup()
        reject(new Error(message.reason ?? 'Transfer cancelled'))
      }
    }
    const onClose = () => {
      cleanup()
      reject(connectionLost())
    }
    const cleanup = () => {
      channel.removeEventListener('message', onMessage)
      channel.removeEventListener('close', onClose)
    }
    channel.addEventListener('message', onMessage)
    channel.addEventListener('close', onClose)
    if (channel.readyState !== 'open') onClose()
  })
}

function waitForFileReady(channel: RTCDataChannel, index: number): Promise<{ offset: number; done: boolean }> {
  return waitForFileControl(channel, index, new Set(['file-ready', 'file-ack'])).then((message) => {
    if (message.type === 'file-ack') return { offset: Number.POSITIVE_INFINITY, done: true }
    if (message.type === 'file-ready') return { offset: message.offset, done: false }
    throw new Error('Unexpected file handshake')
  })
}

function waitForFileAck(channel: RTCDataChannel, index: number): Promise<void> {
  return waitForFileControl(channel, index, new Set(['file-ack'])).then(() => undefined)
}

function encodedSize(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).length
}

function sendBatched<T>(
  channel: RTCDataChannel,
  limit: number,
  start: ControlMessage,
  itemMessage: (batch: T[]) => ControlMessage,
  end: ControlMessage,
  items: T[],
): void {
  sendControl(channel, start)
  let batch: T[] = []
  for (const item of items) {
    batch.push(item)
    if (encodedSize(itemMessage(batch)) > limit) {
      const last = batch.pop()
      if (batch.length) sendControl(channel, itemMessage(batch))
      batch = last === undefined ? [] : [last]
      if (encodedSize(itemMessage(batch)) > limit) {
        throw new Error('A file path is too long to send over the connection')
      }
    }
  }
  if (batch.length) sendControl(channel, itemMessage(batch))
  sendControl(channel, end)
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

function linkAlive(channel: RTCDataChannel, pc?: RTCPeerConnection): boolean {
  if (channel.readyState !== 'open') return false
  const ice = pc?.iceConnectionState
  const conn = pc?.connectionState
  return ice !== 'failed' && conn !== 'failed' && conn !== 'closed'
}

function connectionLost(): Error {
  return new Error('Connection lost. Copied files are kept — continue when both Macs are back on the same Wi-Fi.')
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

function encodeChunk(index: number, offset: number, payload: ArrayBuffer): ArrayBuffer {
  const out = new ArrayBuffer(12 + payload.byteLength)
  const view = new DataView(out)
  view.setUint32(0, index, false)
  view.setBigUint64(4, BigInt(offset), false)
  new Uint8Array(out, 12).set(new Uint8Array(payload))
  return out
}

function decodeChunk(buffer: ArrayBuffer): { index: number; offset: number; payload: Uint8Array } {
  const view = new DataView(buffer)
  return {
    index: view.getUint32(0, false),
    offset: Number(view.getBigUint64(4, false)),
    payload: new Uint8Array(buffer, 12),
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
  pc?: RTCPeerConnection
  inbox: ChannelInbox
  roomCode: string
  files: ScannedFile[]
  manifest: Manifest
  gate: TransferGate
  onProgress: (progress: TransferProgress) => void
}): Promise<void> {
  const { channel, inbox, roomCode, files, manifest, gate, onProgress } = options
  const limit = wireLimit(options.pc)
  const chunkSize = Math.min(CHUNK_SIZE, limit - 12)
  const lanes =
    manifest.verifyChecksums || options.dataChannels.length === 0
      ? [options.dataChannels[0] ?? channel]
      : options.dataChannels
  listenRemoteGate(channel, gate)
  const speed = new Speedometer()
  const progress = throttleProgress(onProgress)
  let checklist: FileProgress[] = emptyChecklist(manifest.files)
  let currentIndex = -1
  let currentSent = 0

  const persist = async (status: 'active' | 'failed' | 'done' = 'active', lastError?: string) => {
    const summary = summarizeChecklist(checklist)
    try {
      await saveSession({
        roomCode,
        role: 'sender',
        manifest,
        fileIndex: Math.max(0, currentIndex),
        fileOffset: currentSent,
        bytesDone: summary.bytesDone,
        verifyChecksums: manifest.verifyChecksums,
        updatedAt: Date.now(),
        status,
        lastError,
        files: leftoverFiles(checklist),
      })
    } catch {
      // Progress lives on the receiving disk. Never stop the folder for a local save error.
    }
  }

  const started = Date.now()
  let sawReady = false
  while (!sawReady) {
    if (Date.now() - started > 15000) throw new Error('Receiver did not become ready')
    const event = await inbox.next()
    if (event.kind === 'control' && event.message.type === 'ready') {
      sawReady = true
      break
    }
    if (event.kind === 'close') throw new Error('Connection closed while waiting for the receiver')
  }

  sendBatched(
    channel,
    limit,
    {
      type: 'manifest-start',
      folderName: manifest.folderName,
      totalBytes: manifest.totalBytes,
      verifyChecksums: manifest.verifyChecksums,
      count: manifest.files.length,
    },
    (batch) => ({ type: 'manifest-part', files: batch }),
    { type: 'manifest-end', count: manifest.files.length },
    manifest.files,
  )

  let inventory: Array<{ path: string; bytes: number }> | undefined
  const inventoryParts: Array<{ path: string; bytes: number }> = []
  let inventoryCount = -1
  const inventoryDeadline = Date.now() + 10 * 60 * 1000
  while (!inventory) {
    if (Date.now() > inventoryDeadline) throw new Error('Receiver did not finish checking every file')
    const event = await inbox.next()
    if (event.kind === 'close') throw new Error('Connection closed while waiting for the file list')
    if (event.kind !== 'control') continue
    if (event.message.type === 'inventory') {
      inventory = event.message.files
      break
    }
    if (event.message.type === 'inventory-start') {
      inventoryParts.length = 0
      inventoryCount = event.message.count
    }
    if (event.message.type === 'inventory-part') {
      inventoryParts.push(...event.message.files)
    }
    if (event.message.type === 'inventory-end') {
      const expected = event.message.count ?? inventoryCount
      if (expected < 0 || inventoryParts.length !== expected) {
        throw new Error(
          `File check was incomplete (${inventoryParts.length} of ${expected} files). Reconnect so every file is checked again.`,
        )
      }
      if (expected !== manifest.files.length) {
        throw new Error(
          `File check missed files (${expected} reported, ${manifest.files.length} in the folder). Reconnect and try again.`,
        )
      }
      inventory = inventoryParts
      break
    }
  }

  checklist = applyInventory(manifest.files, inventory)
  const initial = summarizeChecklist(checklist)
  let bytesDone = initial.bytesDone
  let lastSave = 0
  progress.push(
    {
      fileIndex: 0,
      filePath: initial.done > 0 ? `Skipping ${initial.done} files already copied` : 'Starting transfer',
      fileBytes: 0,
      fileSize: 0,
      bytesDone,
      totalBytes: manifest.totalBytes,
      speed: 0,
      filesDone: initial.done,
      filesTotal: checklist.length,
    },
    true,
  )

  try {
  for (let index = 0; index < files.length; index += 1) {
    const file = files[index]
    const known = checklist[index] ?? {
      path: file.path,
      size: file.size,
      status: 'pending' as const,
      bytes: 0,
    }
    checklist[index] = known
    await gate.waitIfPaused()
    if (known.status === 'done' && known.bytes === file.size) continue

    currentIndex = index
    let copied = false
    let attempt = 0
    while (!copied) {
      attempt += 1
      try {
        await gate.waitIfPaused()
        const hinted = destBytesFor(known)
        currentSent = hinted
        known.status = hinted > 0 && hinted < file.size ? 'partial' : 'pending'
        known.bytes = hinted
        sendControl(channel, { type: 'file-start', index, path: file.path, size: file.size, offset: hinted })
        const ready = await waitForFileReady(channel, index)
        if (ready.done || ready.offset >= file.size) {
          known.status = 'done'
          known.bytes = file.size
          known.error = undefined
          copied = true
          break
        }

        const offset = Math.max(0, Math.min(ready.offset, file.size))
        const blob = await file.handle.getFile()
        if (blob.size !== file.size) {
          throw new Error(`${file.path} changed size while sending. Scan the folder again.`)
        }
        const hasher = manifest.verifyChecksums ? await createHasher() : null
        let sent = offset
        currentSent = offset

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

          for (let inner = 0; inner < block.byteLength; inner += chunkSize) {
            await gate.waitIfPaused()
            const lane = pickChannel(lanes)
            await waitForBuffer(lane)
            const piece = block.slice(inner, Math.min(inner + chunkSize, block.byteLength))
            const fileOffset = sent
            lane.send(encodeChunk(index, fileOffset, piece))
            sent += piece.byteLength
            bytesDone += piece.byteLength
            currentSent = sent
            known.bytes = sent
            known.status = 'partial'
            progress.push({
              fileIndex: index,
              filePath: file.path,
              fileBytes: sent,
              fileSize: file.size,
              bytesDone,
              totalBytes: manifest.totalBytes,
              speed: speed.push(piece.byteLength),
              filesDone: summarizeChecklist(checklist).done,
              filesTotal: checklist.length,
            })
          }
        }

        try {
          await waitUntilDrained(lanes)
        } catch {
          if (!linkAlive(channel, options.pc)) throw connectionLost()
        }
        sendControl(channel, { type: 'file-end', index, sha256: hasher ? await hasher.digest() : undefined })
        await waitForFileAck(channel, index)
        copied = true
      } catch (error) {
        if (!linkAlive(channel, options.pc)) throw connectionLost()
        const message = error instanceof Error ? error.message : 'File failed'
        if (message.startsWith('Connection lost') || message === 'Transfer cancelled') throw error
        known.status = currentSent > 0 ? 'partial' : 'failed'
        known.bytes = currentSent
        known.error = message
        if (error instanceof FileNackError) known.bytes = error.offset
        await persist('active', known.error)
        if (attempt >= 8) {
          throw new Error(`Could not finish ${file.path} after ${attempt} tries. ${message}`)
        }
        await new Promise((resolve) => window.setTimeout(resolve, Math.min(4000, 400 * attempt)))
      }
    }

    known.status = 'done'
    known.bytes = file.size
    known.error = undefined
    currentSent = 0
    const summary = summarizeChecklist(checklist)
    progress.push(
      {
        fileIndex: index,
        filePath: file.path,
        fileBytes: file.size,
        fileSize: file.size,
        bytesDone,
        totalBytes: manifest.totalBytes,
        speed: speed.push(1),
        filesDone: summary.done,
        filesTotal: checklist.length,
      },
      true,
    )

    if (Date.now() - lastSave > 2000 || index === files.length - 1) {
      lastSave = Date.now()
      await persist('active')
    }
  }

  sendControl(channel, { type: 'done' })
  await persist('done')
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Transfer failed'
    if (currentIndex >= 0 && checklist[currentIndex]) {
      const current = checklist[currentIndex]
      current.status = currentSent > 0 ? 'partial' : 'failed'
      current.bytes = currentSent
      current.error = message
    }
    await persist('failed', message)
    throw error
  }
}

export async function receiveFolder(options: {
  channel: RTCDataChannel
  dataChannels: RTCDataChannel[]
  pc?: RTCPeerConnection
  inbox: ChannelInbox
  roomCode: string
  dest: FileSystemDirectoryHandle
  gate: TransferGate
  onManifest: (manifest: Manifest) => void
  onProgress: (progress: TransferProgress) => void
}): Promise<void> {
  const { channel, inbox, roomCode, dest, gate, onManifest, onProgress } = options
  const limit = wireLimit(options.pc)
  const lanes = options.dataChannels.length > 0 ? options.dataChannels : [channel]
  const speed = new Speedometer()
  const progress = throttleProgress(onProgress)
  let checklist: FileProgress[] = []

  sendControl(channel, { type: 'ready' })
  const retry = window.setInterval(() => {
    try {
      sendControl(channel, { type: 'ready' })
    } catch {
      window.clearInterval(retry)
    }
  }, 400)

  const io = {
    writable: null as FileSystemWritableFileStream | null,
  }
  let manifest: Manifest | null = null
  let manifestParts: Manifest['files'] = []
  let pendingManifest: Omit<Manifest, 'files'> | null = null
  let manifestCount = -1
  let currentIndex = -1
  let currentPath = ''
  let currentSize = 0
  let received = 0
  let bytesDone = 0
  let fileReady = false
  let writeQueue = Promise.resolve()
  let ioLock = Promise.resolve()
  let outstanding = 0
  let lastSave = 0
  let pendingEnd: { index: number; sha256?: string } | null = null
  let spans: ByteSpan[] = []
  let holeTimer = 0
  const earlyChunks: Array<{ index: number; offset: number; payload: Uint8Array }> = []

  const runExclusive = (fn: () => Promise<void>) => {
    const next = ioLock.then(fn)
    ioLock = next.catch(() => {})
    return next
  }

  const enqueueWrite = (offset: number, payload: Uint8Array) => {
    outstanding += payload.byteLength
    writeQueue = writeQueue.then(async () => {
      if (!io.writable) throw new Error('Write arrived after the file was closed')
      await io.writable.write({ type: 'write', position: offset, data: payload.slice() })
      outstanding = Math.max(0, outstanding - payload.byteLength)
    })
  }

  const clearHoleTimer = () => {
    if (holeTimer) {
      window.clearTimeout(holeTimer)
      holeTimer = 0
    }
  }

  const nackIncomplete = (reason: string, offset = contiguousEnd(spans, 0)) => {
    pendingEnd = null
    try {
      sendControl(channel, { type: 'file-nack', index: currentIndex, offset, reason })
    } catch {
      // connection may already be down
    }
  }

  const abandonCurrent = async () => {
    clearHoleTimer()
    try {
      await writeQueue
    } catch {
      // ignore
    }
    if (io.writable) {
      try {
        await io.writable.close()
      } catch {
        // ignore
      }
      io.writable = null
    }
    fileReady = false
  }

  const scheduleHoleCheck = () => {
    clearHoleTimer()
    holeTimer = window.setTimeout(() => {
      void runExclusive(async () => {
        if (!pendingEnd || pendingEnd.index !== currentIndex || !fileReady) return
        if (contiguousEnd(spans, 0) >= currentSize) {
          await finishCurrentFile()
          return
        }
        await abandonCurrent()
        nackIncomplete('Missing bytes after the file was sent')
      })
    }, 8000)
  }

  const absorbChunk = (chunk: { offset: number; payload: Uint8Array }) => {
    const before = coveredBytes(spans, 0, currentSize)
    spans = addByteSpan(spans, chunk.offset, chunk.offset + chunk.payload.byteLength)
    const after = coveredBytes(spans, 0, currentSize)
    received = after
    bytesDone += after - before
    enqueueWrite(chunk.offset, chunk.payload)
    if (checklist[currentIndex]) {
      checklist[currentIndex].status = 'partial'
      checklist[currentIndex].bytes = received
    }
  }

  const persistProgress = (status: 'active' | 'failed' | 'done' = 'active') => {
    if (!manifest) return
    lastSave = Date.now()
    const summary = summarizeChecklist(checklist)
    void saveSession({
      roomCode,
      role: 'receiver',
      manifest,
      fileIndex: Math.max(0, currentIndex),
      fileOffset: received,
      bytesDone: summary.bytesDone,
      verifyChecksums: manifest.verifyChecksums,
      updatedAt: Date.now(),
      status,
      files: leftoverFiles(checklist),
    }).catch(() => {})
  }

  const finishCurrentFile = async () => {
    if (!fileReady || !io.writable) return false
    if (currentSize > 0 && contiguousEnd(spans, 0) < currentSize) return false
    clearHoleTimer()
    await writeQueue
    const old = io.writable
    io.writable = null
    fileReady = false
    if (old) await old.close()
    const onDisk = await getFileSize(dest, currentPath)
    if (onDisk !== currentSize) {
      nackIncomplete(`Saved size was ${onDisk}, expected ${currentSize}`, onDisk)
      return false
    }
    if (manifest?.verifyChecksums && pendingEnd?.sha256) {
      const saved = await readDestFile(dest, currentPath)
      if (!saved) {
        nackIncomplete('Could not re-read the saved file', 0)
        return false
      }
      const digest = await hashBlob(saved)
      if (digest !== pendingEnd.sha256) {
        nackIncomplete(`Checksum mismatch for ${currentPath}`, 0)
        return false
      }
    }
    if (checklist[currentIndex]) {
      checklist[currentIndex].status = 'done'
      checklist[currentIndex].bytes = currentSize
    }
    sendControl(channel, { type: 'file-ack', index: currentIndex })
    pendingEnd = null
    spans = []
    if (manifest && (Date.now() - lastSave > 8000 || currentIndex === manifest.files.length - 1)) {
      persistProgress('active')
    }
    return true
  }

  const acceptChunk = async (buffer: ArrayBuffer) => {
    await gate.waitIfPaused()
    await runExclusive(async () => {
      const chunk =
        buffer.byteLength >= 12
          ? decodeChunk(buffer)
          : { index: currentIndex, offset: received, payload: new Uint8Array(buffer) }
      if (fileReady && chunk.index < currentIndex) return
      if (!fileReady || !io.writable || chunk.index !== currentIndex) {
        if (!fileReady || chunk.index >= currentIndex) earlyChunks.push(chunk)
        return
      }
      while (outstanding > WRITE_BACKPRESSURE) {
        await writeQueue
      }
      absorbChunk(chunk)
      if (manifest) {
        progress.push({
          fileIndex: currentIndex,
          filePath: currentPath,
          fileBytes: received,
          fileSize: currentSize,
          bytesDone,
          totalBytes: manifest.totalBytes,
          speed: speed.push(chunk.payload.byteLength),
          filesDone: summarizeChecklist(checklist).done,
          filesTotal: checklist.length,
        })
      }
      if (pendingEnd && pendingEnd.index === currentIndex && contiguousEnd(spans, 0) >= currentSize) {
        await finishCurrentFile()
      } else if (pendingEnd && pendingEnd.index === currentIndex) {
        scheduleHoleCheck()
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
      if (event.kind === 'close') throw connectionLost()

      if (event.kind === 'control') {
        const msg = event.message
        if (msg.type === 'error') throw new Error(msg.message)
        if (msg.type === 'cancel') throw new Error(msg.reason ?? 'Sender cancelled')
        if (msg.type === 'pause') gate.pause()
        if (msg.type === 'resume') gate.resume()
        if (msg.type === 'manifest-start') {
          pendingManifest = {
            folderName: msg.folderName,
            totalBytes: msg.totalBytes,
            verifyChecksums: msg.verifyChecksums,
          }
          manifestCount = msg.count
          manifestParts = []
        }
        if (msg.type === 'manifest-part') {
          manifestParts.push(...msg.files)
        }
        if (msg.type === 'manifest' || msg.type === 'manifest-end') {
          window.clearInterval(retry)
          if (msg.type === 'manifest') {
            manifest = msg.manifest
          } else if (pendingManifest) {
            const expected = msg.count ?? manifestCount
            if (expected >= 0 && manifestParts.length !== expected) {
              throw new Error(
                `Folder list was incomplete (${manifestParts.length} of ${expected} files). Reconnect so every file is checked.`,
              )
            }
            manifest = { ...pendingManifest, files: manifestParts }
          } else {
            throw new Error('Folder list arrived in the wrong order')
          }
          onManifest(manifest)
          onManifest({ ...manifest, folderName: `${manifest.folderName} · checking already-copied files` })
          const present = await inventoryFolder(dest)
          const report = manifest.files.map((file) => ({
            path: file.path,
            bytes: present.get(file.path) ?? 0,
          }))
          checklist = applyInventory(manifest.files, report)
          const summary = summarizeChecklist(checklist)
          bytesDone = summary.bytesDone
          sendBatched(
            channel,
            limit,
            { type: 'inventory-start', count: report.length },
            (batch) => ({ type: 'inventory-part', files: batch }),
            { type: 'inventory-end', count: report.length },
            report,
          )
          progress.push(
            {
              fileIndex: 0,
              filePath:
                summary.done > 0
                  ? `${summary.done} files already on this Mac · ${summary.remaining} left`
                  : `Receiving ${manifest.folderName} · ${manifest.files.length} files`,
              fileBytes: 0,
              fileSize: 0,
              bytesDone,
              totalBytes: manifest.totalBytes,
              speed: 0,
              filesDone: summary.done,
              filesTotal: checklist.length,
            },
            true,
          )
        }
        if (msg.type === 'file-start') {
          await gate.waitIfPaused()
          await runExclusive(async () => {
            if (fileReady && io.writable) {
              const finished = await finishCurrentFile()
              if (!finished) await abandonCurrent()
            } else {
              await abandonCurrent()
            }
            if (!manifest) return
            currentIndex = msg.index
            currentPath = msg.path
            currentSize = msg.size
            pendingEnd = null
            spans = []
            const onDisk = await getFileSize(dest, msg.path)
            if (currentSize === 0 || onDisk === currentSize) {
              if (currentSize === 0) {
                const blank = await openWritable(dest, msg.path, 0)
                await blank.close()
              }
              received = currentSize
              if (checklist[currentIndex]) {
                checklist[currentIndex].status = 'done'
                checklist[currentIndex].bytes = currentSize
              }
              sendControl(channel, { type: 'file-ack', index: currentIndex })
              return
            }
            const offset = onDisk > currentSize ? 0 : onDisk
            spans = offset > 0 ? [{ start: 0, end: offset }] : []
            received = offset
            try {
              io.writable = await openWritable(dest, msg.path, offset)
              fileReady = true
              writeQueue = Promise.resolve()
              outstanding = 0
              const later: typeof earlyChunks = []
              for (const chunk of earlyChunks.splice(0).sort((a, b) => a.offset - b.offset)) {
                if (chunk.index !== currentIndex) {
                  if (chunk.index > currentIndex) later.push(chunk)
                  continue
                }
                absorbChunk(chunk)
              }
              earlyChunks.push(...later)
              sendControl(channel, { type: 'file-ready', index: currentIndex, offset })
            } catch (error) {
              await abandonCurrent()
              const message = error instanceof Error ? error.message : 'Could not open the destination file'
              nackIncomplete(message, offset)
            }
          })
        }
        if (msg.type === 'file-end') {
          pendingEnd = { index: msg.index, sha256: msg.sha256 }
          await runExclusive(async () => {
            if (!pendingEnd || pendingEnd.index !== currentIndex) return
            if (contiguousEnd(spans, 0) >= currentSize) {
              await finishCurrentFile()
              return
            }
            scheduleHoleCheck()
          })
        }
        if (msg.type === 'done') {
          await writeQueue.catch(() => {})
          if (manifest) {
            try {
              await saveSession({
                roomCode,
                role: 'receiver',
                manifest,
                fileIndex: checklist.length,
                fileOffset: 0,
                bytesDone: manifest.totalBytes,
                verifyChecksums: manifest.verifyChecksums,
                updatedAt: Date.now(),
                status: 'done',
                files: leftoverFiles(checklist),
              })
            } catch {
              // ignore
            }
          }
          return
        }
        continue
      }

      await acceptChunk(event.buffer)
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Transfer failed'
    if (checklist[currentIndex]) {
      checklist[currentIndex].status = received > 0 ? 'partial' : 'failed'
      checklist[currentIndex].bytes = received
      checklist[currentIndex].error = message
    }
    if (manifest) {
      const summary = summarizeChecklist(checklist)
      try {
        await saveSession({
          roomCode,
          role: 'receiver',
          manifest,
          fileIndex: currentIndex,
          fileOffset: received,
          bytesDone: summary.bytesDone,
          verifyChecksums: manifest.verifyChecksums,
          updatedAt: Date.now(),
          status: 'failed',
          lastError: message,
          files: leftoverFiles(checklist),
        })
      } catch {
        // ignore
      }
    }
    throw error
  } finally {
    window.clearInterval(retry)
    clearHoleTimer()
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
