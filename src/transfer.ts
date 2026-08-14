import { createHasher } from './hash.ts'
import { openWritable, type ScannedFile } from './fs.ts'
import { saveSession } from './idb.ts'
import {
  BUFFER_HIGH,
  BUFFER_LOW,
  CHUNK_SIZE,
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

export async function sendFolder(options: {
  channel: RTCDataChannel
  inbox: ChannelInbox
  roomCode: string
  files: ScannedFile[]
  manifest: Manifest
  gate: TransferGate
  onProgress: (progress: TransferProgress) => void
}): Promise<void> {
  const { channel, inbox, roomCode, files, manifest, gate, onProgress } = options
  const speed = new Speedometer()

  let readyResume: { index: number; offset: number } | undefined
  const readyDeadline = window.setTimeout(() => {
    /* deadline is checked after each event */
  }, 15000)
  const started = Date.now()
  while (!readyResume) {
    if (Date.now() - started > 15000) {
      window.clearTimeout(readyDeadline)
      throw new Error('Receiver did not become ready')
    }
    const event = await inbox.next()
    if (event.kind === 'control' && event.message.type === 'ready') {
      readyResume = event.message.resumeFrom ?? { index: 0, offset: 0 }
      break
    }
    if (event.kind === 'close') {
      window.clearTimeout(readyDeadline)
      throw new Error('Connection closed while waiting for the receiver')
    }
  }
  window.clearTimeout(readyDeadline)

  sendControl(channel, { type: 'manifest', manifest })

  let bytesDone = 0
  for (let i = 0; i < readyResume.index; i += 1) bytesDone += files[i]?.size ?? 0
  bytesDone += readyResume.offset

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

    for (let pos = offset; pos < blob.size; pos += CHUNK_SIZE) {
      await gate.waitIfPaused()
      const slice = blob.slice(pos, Math.min(pos + CHUNK_SIZE, blob.size))
      const buffer = await slice.arrayBuffer()
      const bytes = new Uint8Array(buffer)
      hasher?.update(bytes)
      await waitForBuffer(channel)
      channel.send(buffer)
      sent += bytes.byteLength
      bytesDone += bytes.byteLength
      onProgress({
        fileIndex: index,
        filePath: file.path,
        fileBytes: sent,
        fileSize: file.size,
        bytesDone,
        totalBytes: manifest.totalBytes,
        speed: speed.push(bytes.byteLength),
      })
    }

    sendControl(channel, { type: 'file-end', index, sha256: hasher ? await hasher.digest() : undefined })
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

  sendControl(channel, { type: 'done' })
}

export async function receiveFolder(options: {
  channel: RTCDataChannel
  inbox: ChannelInbox
  roomCode: string
  dest: FileSystemDirectoryHandle
  gate: TransferGate
  resumeFrom?: { index: number; offset: number }
  onManifest: (manifest: Manifest) => void
  onProgress: (progress: TransferProgress) => void
}): Promise<void> {
  const { channel, inbox, roomCode, dest, gate, onManifest, onProgress } = options
  const speed = new Speedometer()

  sendControl(channel, { type: 'ready', resumeFrom: options.resumeFrom })
  const retry = window.setInterval(() => {
    try {
      sendControl(channel, { type: 'ready', resumeFrom: options.resumeFrom })
    } catch {
      window.clearInterval(retry)
    }
  }, 400)

  let writable: FileSystemWritableFileStream | null = null
  let hasher: Awaited<ReturnType<typeof createHasher>> | null = null
  let manifest: Manifest | null = null
  let currentIndex = options.resumeFrom?.index ?? 0
  let currentPath = ''
  let currentSize = 0
  let received = options.resumeFrom?.offset ?? 0
  let bytesDone = 0

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
          if (writable) {
            await writable.close()
            writable = null
          }
          if (!manifest) throw new Error('file-start arrived before the folder list')
          currentIndex = msg.index
          currentPath = msg.path
          currentSize = msg.size
          received = msg.offset
          hasher = manifest.verifyChecksums ? await createHasher() : null
          writable = await openWritable(dest, msg.path, msg.offset)
        }
        if (msg.type === 'file-end') {
          if (writable) {
            await writable.close()
            writable = null
          }
          if (manifest?.verifyChecksums && msg.sha256 && hasher) {
            const digest = await hasher.digest()
            if (digest !== msg.sha256) throw new Error(`Checksum mismatch for ${currentPath}`)
          }
          if (manifest) {
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
        if (msg.type === 'done') return
        continue
      }

      await gate.waitIfPaused()
      if (!writable || !manifest) throw new Error('Received file data before file-start')
      const bytes = new Uint8Array(event.buffer)
      hasher?.update(bytes)
      await writable.write(bytes)
      received += bytes.byteLength
      bytesDone += bytes.byteLength
      onProgress({
        fileIndex: currentIndex,
        filePath: currentPath,
        fileBytes: received,
        fileSize: currentSize,
        bytesDone,
        totalBytes: manifest.totalBytes,
        speed: speed.push(bytes.byteLength),
      })
    }
  } finally {
    window.clearInterval(retry)
    if (writable) {
      try {
        await writable.close()
      } catch {
        // already closed
      }
    }
  }
}
