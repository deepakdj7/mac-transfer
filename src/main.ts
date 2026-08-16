import './style.css'
import { supportMessage } from './browser.ts'
import { formatBytes, formatDuration, formatSpeed, normalizeRoomCode, randomRoomCode } from './format.ts'
import { leftoverFiles, summarizeChecklist } from './checklist.ts'
import { scanFolder, summarizeScan, type ScannedFile } from './fs.ts'
import { clearSession, ensureHandlePermission, getHandle, getSession, latestSession, saveHandle, saveSession } from './idb.ts'
import { startPeer, type PeerConnection } from './peer.ts'
import { joinLobby, type Lobby, type LobbyInvite, type LobbyPeer } from './presence.ts'
import { joinSignalRoom, type SignalingRoom } from './signaling.ts'
import { ChannelInbox, receiveFolder, sendControl, sendFolder, TransferGate } from './transfer.ts'
import type { FileProgress, Manifest, Role, SessionRecord, SignalMessage, TransferProgress } from './types.ts'
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
let lastRole: Role | null = null
let stopAutoResume = false
let lobby: Lobby | null = null
let nearby: LobbyPeer[] = []
let pendingInvite: LobbyInvite | null = null
let busy = false
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

function renderFileLog(target: HTMLElement, files: FileProgress[] | undefined, extra = 0): void {
  target.replaceChildren()
  if (!files || files.length === 0) return
  for (const file of files) {
    const item = document.createElement('li')
    const path = document.createElement('span')
    path.className = 'path'
    path.textContent = file.path
    const meta = document.createElement('span')
    meta.className = 'meta'
    const label = file.status === 'failed' ? 'failed' : file.status === 'partial' ? `${formatBytes(file.bytes)} in` : file.status
    meta.textContent = `${label} · ${formatBytes(file.size)}`
    item.append(path, meta)
    target.append(item)
  }
  if (extra > 0) {
    const more = document.createElement('li')
    more.textContent = `and ${extra} more still needed`
    target.append(more)
  }
}

function recoverableError(error: unknown): boolean {
  if (stopAutoResume) return false
  const message = error instanceof Error ? error.message : String(error)
  if (
    message === 'Transfer cancelled' ||
    message === 'Transfer cancelled.' ||
    message.includes('Cancelled by') ||
    message.includes('That folder is empty') ||
    message.includes('Could not read') ||
    message.includes('changed size') ||
    message.includes('A file path is too long')
  ) {
    return false
  }
  return true
}

function showReconnecting(attempt: number): void {
  show('transfer')
  $('transfer-file').textContent =
    attempt <= 1
      ? 'Connection dropped. Reconnecting on its own…'
      : `Still reconnecting automatically… try ${attempt}`
  $('xfer-speed').textContent = 'Reconnecting'
  $('xfer-eta').textContent = '—'
}

async function waitForRetry(attempt: number): Promise<void> {
  const delay = Math.min(15000, 800 * 2 ** Math.min(attempt, 4))
  await new Promise<void>((resolve) => {
    let settled = false
    const done = () => {
      if (settled) return
      settled = true
      window.clearTimeout(timer)
      document.removeEventListener('visibilitychange', onVisible)
      window.removeEventListener('online', done)
      resolve()
    }
    const onVisible = () => {
      if (document.visibilityState === 'visible') done()
    }
    const timer = window.setTimeout(done, delay)
    document.addEventListener('visibilitychange', onVisible)
    window.addEventListener('online', done)
  })
}

async function closeLink(): Promise<void> {
  peer?.close()
  peer = null
  signaling?.close()
  signaling = null
  signalHandler = null
  bufferedSignals.length = 0
}

function fail(error: unknown): void {
  stopAutoResume = true
  const message = error instanceof Error ? error.message : 'Something went wrong'
  $('error-body').textContent = message
  show('error')
  void teardown(false)
  void (async () => {
    if (!roomCode) return
    const session = await getSession(roomCode)
    if (!session?.files?.length) {
      $('error-summary').textContent = 'Progress was saved. Continue to pick up only the files that are still missing.'
      renderFileLog($('error-remaining'), [])
      return
    }
    const summary = summarizeChecklist(session.files)
    $('error-summary').textContent =
      `${summary.done.toLocaleString()} of ${summary.total.toLocaleString()} files are already copied · ${summary.remaining.toLocaleString()} left` +
      (summary.failed ? ` · ${summary.failed} failed` : '')
    const leftover = leftoverFiles(session.files)
    renderFileLog($('error-remaining'), leftover.slice(0, 80), Math.max(0, leftover.length - 80))
    await saveSession({ ...session, status: 'failed', lastError: message, updatedAt: Date.now() })
  })()
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
  if (progress.filesTotal > 0) {
    $('xfer-files').textContent =
      `${progress.filesDone.toLocaleString()} of ${progress.filesTotal.toLocaleString()} files copied`
  }
}

async function connect(nextRole: Role, code: string, reconnecting = false): Promise<void> {
  lastRole = nextRole
  roomCode = code
  if (nextRole === 'receiver' || reconnecting) {
    show(reconnecting ? 'transfer' : 'connecting')
    const text = reconnecting
      ? 'Reconnecting to the other Mac…'
      : 'Handshaking, then opening a direct Wi-Fi link…'
    $('connect-status').textContent = text
    if (reconnecting) $('transfer-file').textContent = text
  }

  let releasePeer = () => {}
  const peerPresent = new Promise<void>((resolve) => {
    releasePeer = resolve
  })
  let failConnect = (_error: Error) => {}
  const connectionDied = new Promise<never>((_, reject) => {
    failConnect = reject
  })

  signaling = await joinSignalRoom(code, nextRole, {
    onPeerPresent: () => {
      $('connect-status').textContent = 'Other Mac is here. Opening a direct link…'
      $('wait-status').textContent = 'Other Mac is here. Opening a direct link…'
      if (reconnecting) $('transfer-file').textContent = 'Other Mac is here. Opening a direct link…'
      releasePeer()
    },
    onSignal: (message) => {
      if (signalHandler) signalHandler(message)
      else bufferedSignals.push(message)
    },
    onStatus: (text) => {
      $('connect-status').textContent = text
      $('wait-status').textContent = text
      if (reconnecting) $('transfer-file').textContent = text
    },
    onError: (error) => {
      if (peer) return
      failConnect(error)
    },
  })

  if (nextRole === 'sender') {
    if (!reconnecting) $('wait-status').textContent = 'Waiting for the other Mac…'
    await Promise.race([peerPresent, connectionDied])
    if (!reconnecting) {
      show('connecting')
      $('connect-status').textContent = 'Other Mac is here. Opening a direct link…'
    }
  }

  peer = await Promise.race([
    startPeer(nextRole, signaling, attachSignal, (text) => {
      $('connect-status').textContent = text
      if (reconnecting) $('transfer-file').textContent = text
    }),
    connectionDied,
  ])
}

async function beginSend(existingCode?: string, nearbyName?: string): Promise<void> {
  if (!sourceHandle || scanned.length === 0) return
  const code = existingCode ?? randomRoomCode()
  roomCode = code
  $('room-code').textContent = code
  const link = joinLink(code)
  const anchor = $('join-link') as HTMLAnchorElement
  anchor.href = link
  anchor.textContent = link
  if (nearbyName) {
    $('wait-title').textContent = `Sending to ${nearbyName}`
    $('wait-lede').textContent = 'The other Mac should accept on its own if it is ready to receive. Keep this tab open.'
    $('join-link-wrap').classList.add('hidden')
    $('room-code').parentElement?.classList.add('hidden')
  } else {
    $('wait-title').textContent = 'On the other Mac, join this room'
    $('wait-lede').textContent = 'Open Mac Transfer in Chrome or Edge, tap Receive, and type this code.'
    $('join-link-wrap').classList.remove('hidden')
    $('room-code').parentElement?.classList.remove('hidden')
  }
  show('send-wait')

  const verify = ($('chk-verify') as HTMLInputElement).checked
  const manifest: Manifest = {
    folderName: sourceHandle.name,
    files: scanned.map(({ path, size }) => ({ path, size })),
    totalBytes: scanned.reduce((sum, file) => sum + file.size, 0),
    verifyChecksums: verify,
  }

  const existing = await getSession(code)
  await saveHandle(code, 'source', sourceHandle)
  await saveSession({
    roomCode: code,
    role: 'sender',
    manifest,
    fileIndex: existing?.fileIndex ?? 0,
    fileOffset: existing?.fileOffset ?? 0,
    bytesDone: existing?.bytesDone ?? 0,
    verifyChecksums: verify,
    updatedAt: Date.now(),
    status: 'active',
    files: existing?.files,
  })

  stopAutoResume = false
  wake = new WakeGuard()
  await wake.start()
  let attempt = 0
  try {
    while (!stopAutoResume) {
      try {
        if (attempt > 0) {
          showReconnecting(attempt)
          await closeLink()
          await waitForRetry(attempt)
          if (stopAutoResume) throw new Error('Transfer cancelled')
        }
        await connect('sender', code, attempt > 0)
        const inbox = new ChannelInbox(peer!.channel)
        gate = new TransferGate()
        show('transfer')
        $('transfer-title').textContent = 'Sending'
        $('btn-pause').textContent = 'Pause'
        await sendFolder({
          channel: peer!.channel,
          dataChannels: peer!.dataChannels,
          pc: peer!.pc,
          inbox,
          roomCode: code,
          files: scanned,
          manifest,
          gate,
          onProgress: renderProgress,
        })
        break
      } catch (error) {
        if (!recoverableError(error)) throw error
        attempt += 1
      }
    }
    if (stopAutoResume) throw new Error('Transfer cancelled')
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
    verifyChecksums: resume?.verifyChecksums ?? false,
    updatedAt: Date.now(),
    status: 'active',
    files: resume?.files,
  })

  stopAutoResume = false
  wake = new WakeGuard()
  await wake.start()
  let attempt = 0
  try {
    while (!stopAutoResume) {
      try {
        if (attempt > 0) {
          showReconnecting(attempt)
          await closeLink()
          await waitForRetry(attempt)
          if (stopAutoResume) throw new Error('Transfer cancelled')
        }
        await connect('receiver', code, attempt > 0)
        const inbox = new ChannelInbox(peer!.channel)
        gate = new TransferGate()
        show('transfer')
        $('transfer-title').textContent = 'Receiving'
        $('btn-pause').textContent = 'Pause'
        await receiveFolder({
          channel: peer!.channel,
          dataChannels: peer!.dataChannels,
          pc: peer!.pc,
          inbox,
          roomCode: code,
          dest: destHandle,
          gate,
          onManifest: (manifest) => {
            $('transfer-file').textContent = `Receiving ${manifest.folderName} · ${manifest.files.length} files`
          },
          onProgress: renderProgress,
        })
        break
      } catch (error) {
        if (!recoverableError(error)) throw error
        attempt += 1
      }
    }
    if (stopAutoResume) throw new Error('Transfer cancelled')
    await clearSession(code)
    $('done-title').textContent = 'Received'
    $('done-body').textContent = `The folder is on this Mac. Keep the destination window handy if you want to check it.`
    show('done')
    await teardown(false)
  } catch (error) {
    fail(error)
  }
}

function initials(name: string): string {
  return name
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part[0] ?? '')
    .join('')
    .toUpperCase()
}

function renderLobby(): void {
  const grid = $('peer-grid')
  grid.replaceChildren()
  const cards: Array<LobbyPeer & { you?: boolean }> = lobby ? [{ ...lobby.me, you: true }, ...nearby] : nearby
  if (cards.length === 0) {
    const empty = document.createElement('p')
    empty.className = 'hint'
    empty.textContent = 'No other Mac yet. Keep this tab open on both computers.'
    grid.append(empty)
    return
  }
  for (const peer of cards) {
    const button = document.createElement('button')
    button.type = 'button'
    button.className = peer.you ? 'peer you' : 'peer'
    button.dataset.id = peer.id
    const avatar = document.createElement('span')
    avatar.className = 'avatar'
    avatar.style.setProperty('--hue', String(peer.hue))
    avatar.textContent = initials(peer.name)
    const name = document.createElement('span')
    name.className = 'peer-name'
    name.textContent = peer.name
    const meta = document.createElement('span')
    meta.className = 'peer-meta'
    meta.textContent = peer.you
      ? destHandle
        ? 'You · ready to receive'
        : 'You · click Ready to receive'
      : peer.ready
        ? 'Ready · click or drop a folder'
        : 'Click to send a folder'
    button.append(avatar, name, meta)
    if (!peer.you) {
      button.addEventListener('click', () => {
        void sendToPeer(peer).catch(ignoreAbort)
      })
      button.addEventListener('dragover', (event) => {
        event.preventDefault()
        button.classList.add('drop')
      })
      button.addEventListener('dragleave', () => button.classList.remove('drop'))
      button.addEventListener('drop', (event) => {
        event.preventDefault()
        button.classList.remove('drop')
        void sendDroppedFolder(peer, event.dataTransfer).catch(ignoreAbort)
      })
    }
    grid.append(button)
  }
}

function ignoreAbort(error: unknown): void {
  if (error instanceof DOMException && error.name === 'AbortError') return
  fail(error)
}

function showIncoming(invite: LobbyInvite): void {
  pendingInvite = invite
  $('incoming-text').textContent = `${invite.fromName} wants to send a folder.`
  $('incoming').classList.remove('hidden')
}

async function acceptInvite(invite: LobbyInvite): Promise<void> {
  if (busy) return
  if (!destHandle) {
    destHandle = await pickDest()
    if (!destHandle) return
    await saveHandle('lobby', 'dest', destHandle)
    lobby?.setReady(true)
    $('btn-ready').textContent = 'Receiving folder is set'
  }
  pendingInvite = null
  $('incoming').classList.add('hidden')
  busy = true
  try {
    await beginReceive(invite.room, await getSession(invite.room))
  } finally {
    busy = false
  }
}

async function scanSource(handle: FileSystemDirectoryHandle): Promise<boolean> {
  sourceHandle = handle
  show('send-scan')
  $('scan-status').textContent = `Reading “${handle.name}”…`
  scanned = await scanFolder(
    handle,
    (info) => {
      $('scan-status').textContent = `Found ${info.fileCount} files · ${formatBytes(info.totalBytes)}`
    },
    { failOnUnreadable: true },
  )
  if (scanned.length === 0) {
    fail(new Error('That folder is empty, or every file was unreadable.'))
    return false
  }
  const total = scanned.reduce((sum, file) => sum + file.size, 0)
  $('send-folder-name').textContent = handle.name
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
  return true
}

async function sendToPeer(peer: LobbyPeer, handle?: FileSystemDirectoryHandle): Promise<void> {
  if (busy) return
  const support = supportMessage()
  if (support) {
    setWarn(support)
    return
  }
  const folder = handle ?? (await window.showDirectoryPicker({ mode: 'read', id: 'mac-transfer-source' }))
  if (!(await scanSource(folder))) return
  const code = randomRoomCode()
  lobby?.invite(peer.id, code)
  busy = true
  try {
    await beginSend(code, peer.name)
  } finally {
    busy = false
  }
}

async function sendDroppedFolder(peer: LobbyPeer, transfer: DataTransfer | null): Promise<void> {
  if (!transfer) return
  for (const item of transfer.items) {
    const handle = await item.getAsFileSystemHandle?.()
    if (handle?.kind === 'directory') {
      await sendToPeer(peer, handle as FileSystemDirectoryHandle)
      return
    }
  }
  setWarn('Drop a folder, not a single file.')
}

async function startLobby(): Promise<void> {
  if (lobby) return
  try {
    lobby = await joinLobby({
      onPeers(peers) {
        nearby = peers
        renderLobby()
      },
      onInvite(invite) {
        if (busy) return
        if (destHandle) {
          void acceptInvite(invite).catch(ignoreAbort)
          return
        }
        showIncoming(invite)
      },
      onStatus(text) {
        $('lobby-status').textContent = text
      },
    })
    if (destHandle) lobby.setReady(true)
    renderLobby()
  } catch (error) {
    $('lobby-status').textContent =
      'Could not find nearby devices. Use a room code, or check that both Macs can reach the internet for the handshake.'
    console.warn(error)
  }
}

async function pickAndScan(): Promise<void> {
  const support = supportMessage()
  if (support) {
    setWarn(support)
    return
  }
  const handle = await window.showDirectoryPicker({ mode: 'read', id: 'mac-transfer-source' })
  if (await scanSource(handle)) show('send-ready')
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
  const summary = session.files?.length ? summarizeChecklist(session.files) : null
  $('resume-text').textContent = summary
    ? `${session.role === 'sender' ? 'Sending' : 'Receiving'} room ${session.roomCode}. ${summary.done.toLocaleString()} of ${summary.total.toLocaleString()} files already copied · ${summary.remaining.toLocaleString()} left.`
    : `${session.role === 'sender' ? 'Sending' : 'Receiving'} room ${session.roomCode} · ${formatBytes(session.bytesDone)} already moved.`
  const leftover = session.files ? leftoverFiles(session.files) : []
  renderFileLog(
    $('resume-remaining'),
    leftover.slice(0, 80),
    Math.max(0, leftover.length - 80),
  )
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
    scanned = await scanFolder(handle, undefined, { failOnUnreadable: true })
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
  stopAutoResume = true
  busy = false
  pendingInvite = null
  $('incoming').classList.add('hidden')
  void teardown(false)
  sourceHandle = null
  scanned = []
  roomCode = ''
  show('home')
  void loadResumeCard()
  renderLobby()
  void startLobby()
}

$('btn-ready').addEventListener('click', () => {
  void (async () => {
    destHandle = await pickDest()
    if (!destHandle) return
    await saveHandle('lobby', 'dest', destHandle)
    lobby?.setReady(true)
    $('btn-ready').textContent = 'Receiving folder is set'
    renderLobby()
    if (pendingInvite) await acceptInvite(pendingInvite)
  })().catch(ignoreAbort)
})

$('btn-accept').addEventListener('click', () => {
  if (pendingInvite) void acceptInvite(pendingInvite).catch(ignoreAbort)
})

$('btn-send').addEventListener('click', () => {
  void pickAndScan().catch(ignoreAbort)
})

$('btn-recv').addEventListener('click', () => {
  show('recv-join')
  $('join-code').focus()
})

$('btn-send-cancel').addEventListener('click', resetHome)
$('btn-recv-cancel').addEventListener('click', resetHome)
$('btn-again').addEventListener('click', resetHome)
$('btn-error-home').addEventListener('click', () => {
  if (roomCode) void clearSession(roomCode)
  resetHome()
})

$('btn-continue').addEventListener('click', () => {
  void (async () => {
    await teardown(false)
    const session = roomCode ? await getSession(roomCode) : await latestSession()
    const role = lastRole ?? session?.role
    if (!session || !role) {
      fail(new Error('No saved transfer to continue. Use the same destination folder and room code.'))
      return
    }
    roomCode = session.roomCode
    lastRole = role
    if (role === 'sender') {
      if (!sourceHandle) {
        await resumeLast()
        return
      }
      await beginSend(session.roomCode)
      return
    }
    if (!destHandle) {
      await resumeLast()
      return
    }
    await beginReceive(session.roomCode, session)
  })().catch((error) => {
    if (error instanceof DOMException && error.name === 'AbortError') return
    fail(error)
  })
})

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
  stopAutoResume = true
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

void (async () => {
  const saved = await getHandle('lobby', 'dest')
  if (saved && (await ensureHandlePermission(saved, 'readwrite'))) {
    destHandle = saved
    $('btn-ready').textContent = 'Receiving folder is set'
  }
  await loadResumeCard()
  await startLobby()
})()
