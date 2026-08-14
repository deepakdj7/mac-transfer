import './style.css'
import { supportMessage } from './browser.ts'
import { formatBytes, formatDuration, formatSpeed, normalizeRoomCode, randomRoomCode } from './format.ts'
import { scanFolder, summarizeScan, type ScannedFile } from './fs.ts'
import { clearSession, ensureHandlePermission, getHandle, getSession, latestSession, saveHandle, saveSession } from './idb.ts'
import { startPeer, type PeerConnection } from './peer.ts'
import { joinSignalRoom, type SignalingRoom } from './signaling.ts'
import { ChannelInbox, receiveFolder, sendControl, sendFolder, TransferGate } from './transfer.ts'
import type { Manifest, Role, SessionRecord, SignalMessage, TransferProgress } from './types.ts'
import { WakeGuard } from './wake.ts'

const views = [
  'home',
  'send-scan',
  'send-ready',
  'send-wait',
  'recv-join',
  'connecting',
  'transfer',
  'done',
  'error',
] as const

type View = (typeof views)[number]

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T

let sourceHandle: FileSystemDirectoryHandle | null = null
let destHandle: FileSystemDirectoryHandle | null = null
let scanned: ScannedFile[] = []
let roomCode = ''
let signaling: SignalingRoom | null = null
let peer: PeerConnection | null = null
let gate: TransferGate | null = null
let wake: WakeGuard | null = null
let signalHandler: ((message: SignalMessage) => void) | null = null
const bufferedSignals: SignalMessage[] = []

function show(view: View): void {
  for (const name of views) {
    $(`view-${name}`).classList.toggle('hidden', name !== view)
  }
}

function setWarn(text: string | null): void {
  const el = $('warn')
  el.textContent = text ?? ''
  el.classList.toggle('hidden', !text)
}

function fail(error: unknown): void {
  const message = error instanceof Error ? error.message : 'Something went wrong'
  $('error-body').textContent = message
  show('error')
  void teardown(false)
}

async function teardown(clear = false): Promise<void> {
  gate?.cancel()
  gate = null
  await wake?.stop()
  wake = null
  peer?.close()
  peer = null
  signaling?.close()
  signaling = null
  signalHandler = null
  bufferedSignals.length = 0
  if (clear && roomCode) await clearSession(roomCode)
}

function attachSignal(handler: (message: SignalMessage) => void): void {
  signalHandler = handler
  for (const message of bufferedSignals.splice(0)) handler(message)
}

function joinLink(code: string): string {
  const url = new URL(location.href)
  url.searchParams.set('join', code)
  return url.toString()
}

function renderProgress(progress: TransferProgress): void {
  const pct = progress.totalBytes === 0 ? 100 : (progress.bytesDone / progress.totalBytes) * 100
  $('transfer-file').textContent = progress.filePath || 'Preparing files…'
  $('bar-fill').style.width = `${Math.min(100, pct)}%`
  $('xfer-pct').textContent = `${pct.toFixed(1)}%`
  $('xfer-bytes').textContent = `${formatBytes(progress.bytesDone)} / ${formatBytes(progress.totalBytes)}`
  $('xfer-speed').textContent = formatSpeed(progress.speed)
  const remain = progress.totalBytes - progress.bytesDone
  $('xfer-eta').textContent = progress.speed > 0 ? formatDuration(remain / progress.speed) : '—'
}

async function connect(nextRole: Role, code: string): Promise<void> {
  roomCode = code
  if (nextRole === 'receiver') {
    show('connecting')
    $('connect-status').textContent = 'Handshaking, then opening a direct Wi-Fi link…'
  }

  let releasePeer = () => {}
  const peerPresent = new Promise<void>((resolve) => {
    releasePeer = resolve
  })

  signaling = await joinSignalRoom(code, nextRole, {
    onPeerPresent: () => {
      $('connect-status').textContent = 'Other Mac is here. Opening a direct link…'
      $('wait-status').textContent = 'Other Mac is here. Opening a direct link…'
      releasePeer()
    },
    onSignal: (message) => {
      if (signalHandler) signalHandler(message)
      else bufferedSignals.push(message)
    },
    onStatus: (text) => {
      $('connect-status').textContent = text
      $('wait-status').textContent = text
    },
    onError: (error) => fail(error),
  })

  if (nextRole === 'sender') {
    $('wait-status').textContent = 'Waiting for the other Mac…'
    await peerPresent
    show('connecting')
    $('connect-status').textContent = 'Other Mac is here. Opening a direct link…'
  }

  peer = await startPeer(nextRole, signaling, attachSignal, (text) => {
    $('connect-status').textContent = text
  })
}

async function beginSend(existingCode?: string): Promise<void> {
  if (!sourceHandle || scanned.length === 0) return
  const code = existingCode ?? randomRoomCode()
  roomCode = code
  $('room-code').textContent = code
  const link = joinLink(code)
  const anchor = $('join-link') as HTMLAnchorElement
  anchor.href = link
  anchor.textContent = link
  show('send-wait')

  const verify = ($('chk-verify') as HTMLInputElement).checked
  const manifest: Manifest = {
    folderName: sourceHandle.name,
    files: scanned.map(({ path, size }) => ({ path, size })),
    totalBytes: scanned.reduce((sum, file) => sum + file.size, 0),
    verifyChecksums: verify,
  }

  await saveHandle(code, 'source', sourceHandle)
  await saveSession({
    roomCode: code,
    role: 'sender',
    manifest,
    fileIndex: 0,
    fileOffset: 0,
    bytesDone: 0,
    verifyChecksums: verify,
    updatedAt: Date.now(),
  })

  try {
    await connect('sender', code)
    const inbox = new ChannelInbox(peer!.channel)
    gate = new TransferGate()
    wake = new WakeGuard()
    await wake.start()
    show('transfer')
    $('transfer-title').textContent = 'Sending'
    $('btn-pause').textContent = 'Pause'
    await sendFolder({
      channel: peer!.channel,
      inbox,
      roomCode: code,
      files: scanned,
      manifest,
      gate,
      onProgress: renderProgress,
    })
    await clearSession(code)
    $('done-title').textContent = 'Sent'
    $('done-body').textContent = `${manifest.files.length} files (${formatBytes(manifest.totalBytes)}) reached the other Mac.`
    show('done')
    await teardown(false)
  } catch (error) {
    fail(error)
  }
}

async function beginReceive(code: string, resume?: SessionRecord): Promise<void> {
  if (!destHandle) return
  roomCode = code
  await saveHandle(code, 'dest', destHandle)
  await saveSession({
    roomCode: code,
    role: 'receiver',
    manifest: resume?.manifest,
    fileIndex: resume?.fileIndex ?? 0,
    fileOffset: resume?.fileOffset ?? 0,
    bytesDone: resume?.bytesDone ?? 0,
    verifyChecksums: resume?.verifyChecksums ?? true,
    updatedAt: Date.now(),
  })

  try {
    await connect('receiver', code)
    const inbox = new ChannelInbox(peer!.channel)
    gate = new TransferGate()
    wake = new WakeGuard()
    await wake.start()
    show('transfer')
    $('transfer-title').textContent = 'Receiving'
    $('btn-pause').textContent = 'Pause'
    await receiveFolder({
      channel: peer!.channel,
      inbox,
      roomCode: code,
      dest: destHandle,
      gate,
      resumeFrom:
        resume && resume.fileIndex + resume.fileOffset > 0
          ? { index: resume.fileIndex, offset: resume.fileOffset }
          : undefined,
      onManifest: (manifest) => {
        $('transfer-file').textContent = `Receiving ${manifest.folderName} · ${manifest.files.length} files`
      },
      onProgress: renderProgress,
    })
    await clearSession(code)
    $('done-title').textContent = 'Received'
    $('done-body').textContent = `The folder is on this Mac. Keep the destination window handy if you want to check it.`
    show('done')
    await teardown(false)
  } catch (error) {
    fail(error)
  }
}

async function pickAndScan(): Promise<void> {
  const support = supportMessage()
  if (support) {
    setWarn(support)
    return
  }
  sourceHandle = await window.showDirectoryPicker({ mode: 'read', id: 'mac-transfer-source' })
  show('send-scan')
  $('scan-status').textContent = `Reading “${sourceHandle.name}”…`
  scanned = await scanFolder(sourceHandle, (info) => {
    $('scan-status').textContent = `Found ${info.fileCount} files · ${formatBytes(info.totalBytes)}`
  })
  if (scanned.length === 0) {
    fail(new Error('That folder is empty, or every file was unreadable.'))
    return
  }
  const total = scanned.reduce((sum, file) => sum + file.size, 0)
  $('send-folder-name').textContent = sourceHandle.name
  $('send-file-count').textContent = String(scanned.length)
  $('send-total-size').textContent = formatBytes(total)
  const summary = summarizeScan(scanned)
  const hints: string[] = []
  if (scanned.length > 20000) {
    hints.push('This folder has a huge number of files. Many tiny files are slower than a few large ones.')
  } else if (summary.tinyFiles > 5000) {
    hints.push('Lots of tiny files. The transfer will work, but it will spend more time on overhead.')
  }
  if (summary.hugeFiles > 0) {
    hints.push('At least one file is 10 GB or larger. Keep this tab visible until it finishes writing.')
  }
  $('send-hint').textContent = hints.join(' ')
  show('send-ready')
}

async function pickDest(): Promise<FileSystemDirectoryHandle | null> {
  const support = supportMessage()
  if (support) {
    setWarn(support)
    return null
  }
  return window.showDirectoryPicker({ mode: 'readwrite', id: 'mac-transfer-dest' })
}

async function loadResumeCard(): Promise<void> {
  const session = await latestSession()
  const card = $('resume-card')
  if (!session) {
    card.classList.add('hidden')
    return
  }
  $('resume-text').textContent =
    `${session.role === 'sender' ? 'Sending' : 'Receiving'} room ${session.roomCode} · ${formatBytes(session.bytesDone)} already moved.`
  card.classList.remove('hidden')
  card.dataset.room = session.roomCode
}

async function resumeLast(): Promise<void> {
  const session = await latestSession()
  if (!session) return
  roomCode = session.roomCode
  if (session.role === 'sender') {
    const handle = await getHandle(session.roomCode, 'source')
    if (!handle || !(await ensureHandlePermission(handle, 'read'))) {
      await pickAndScan()
      await beginSend(session.roomCode)
      return
    }
    sourceHandle = handle
    show('send-scan')
    scanned = await scanFolder(handle)
    ;($('chk-verify') as HTMLInputElement).checked = session.verifyChecksums
    await beginSend(session.roomCode)
    return
  }

  const handle = await getHandle(session.roomCode, 'dest')
  if (!handle || !(await ensureHandlePermission(handle, 'readwrite'))) {
    destHandle = await pickDest()
    if (!destHandle) return
  } else {
    destHandle = handle
  }
  await beginReceive(session.roomCode, session)
}

function resetHome(): void {
  void teardown(false)
  sourceHandle = null
  destHandle = null
  scanned = []
  roomCode = ''
  show('home')
  void loadResumeCard()
}

$('btn-send').addEventListener('click', () => {
  void pickAndScan().catch((error) => {
    if (error instanceof DOMException && error.name === 'AbortError') return
    fail(error)
  })
})

$('btn-recv').addEventListener('click', () => {
  show('recv-join')
  $('join-code').focus()
})

$('btn-send-cancel').addEventListener('click', resetHome)
$('btn-recv-cancel').addEventListener('click', resetHome)
$('btn-again').addEventListener('click', resetHome)
$('btn-error-home').addEventListener('click', resetHome)

$('btn-create-room').addEventListener('click', () => {
  void beginSend()
})

$('btn-copy').addEventListener('click', async () => {
  try {
    await navigator.clipboard.writeText(roomCode)
    $('btn-copy').textContent = 'Copied'
    window.setTimeout(() => {
      $('btn-copy').textContent = 'Copy'
    }, 1200)
  } catch {
    $('btn-copy').textContent = 'Copy failed'
  }
})

$('btn-join').addEventListener('click', () => {
  const code = normalizeRoomCode(($('join-code') as HTMLInputElement).value)
  if (code.length < 4) {
    setWarn('Enter the full room code from the sending Mac.')
    return
  }
  void (async () => {
    destHandle = await pickDest()
    if (!destHandle) return
    const existing = await getSession(code)
    await beginReceive(code, existing)
  })().catch((error) => {
    if (error instanceof DOMException && error.name === 'AbortError') return
    fail(error)
  })
})

$('btn-pause').addEventListener('click', () => {
  if (!gate || !peer) return
  if (gate.paused) {
    gate.resume()
    try {
      sendControl(peer.channel, { type: 'resume' })
    } catch {
      // ignore
    }
    $('btn-pause').textContent = 'Pause'
    return
  }
  gate.pause()
  try {
    sendControl(peer.channel, { type: 'pause' })
  } catch {
    // ignore
  }
  $('btn-pause').textContent = 'Resume'
})

$('btn-cancel').addEventListener('click', () => {
  try {
    if (peer) sendControl(peer.channel, { type: 'cancel', reason: 'Cancelled by the other Mac' })
  } catch {
    // ignore
  }
  gate?.cancel()
  fail(new Error('Transfer cancelled.'))
})

$('btn-resume').addEventListener('click', () => {
  void resumeLast().catch((error) => {
    if (error instanceof DOMException && error.name === 'AbortError') return
    fail(error)
  })
})

$('btn-resume-dismiss').addEventListener('click', () => {
  const code = $('resume-card').dataset.room
  if (code) void clearSession(code)
  $('resume-card').classList.add('hidden')
})

const support = supportMessage()
if (support) setWarn(support)

const preset = normalizeRoomCode(new URL(location.href).searchParams.get('join') ?? '')
if (preset) {
  show('recv-join')
  ;($('join-code') as HTMLInputElement).value = preset
}

void loadResumeCard()
