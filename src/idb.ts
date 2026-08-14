import type { SessionRecord } from './types.ts'

const DB_NAME = 'mac-transfer'
const DB_VERSION = 1
const SESSION_STORE = 'sessions'
const HANDLE_STORE = 'handles'

function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION)
    req.onerror = () => reject(req.error ?? new Error('Failed to open IndexedDB'))
    req.onsuccess = () => resolve(req.result)
    req.onupgradeneeded = () => {
      const db = req.result
      if (!db.objectStoreNames.contains(SESSION_STORE)) {
        db.createObjectStore(SESSION_STORE, { keyPath: 'roomCode' })
      }
      if (!db.objectStoreNames.contains(HANDLE_STORE)) {
        db.createObjectStore(HANDLE_STORE)
      }
    }
  })
}

function reqToPromise<T>(req: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error ?? new Error('IndexedDB request failed'))
  })
}

export async function saveSession(session: SessionRecord): Promise<void> {
  const db = await openDb()
  const tx = db.transaction(SESSION_STORE, 'readwrite')
  await reqToPromise(tx.objectStore(SESSION_STORE).put({ ...session, roomCode: session.roomCode.toUpperCase() }))
  db.close()
}

export async function patchSession(roomCode: string, patch: Partial<SessionRecord>): Promise<SessionRecord | undefined> {
  const current = await getSession(roomCode)
  if (!current) return undefined
  const next = { ...current, ...patch, roomCode: roomCode.toUpperCase(), updatedAt: Date.now() }
  await saveSession(next)
  return next
}

export async function getSession(roomCode: string): Promise<SessionRecord | undefined> {
  const db = await openDb()
  const tx = db.transaction(SESSION_STORE, 'readonly')
  const value = await reqToPromise(tx.objectStore(SESSION_STORE).get(roomCode.toUpperCase()))
  db.close()
  return value as SessionRecord | undefined
}

export async function latestSession(): Promise<SessionRecord | undefined> {
  const db = await openDb()
  const tx = db.transaction(SESSION_STORE, 'readonly')
  const values = (await reqToPromise(tx.objectStore(SESSION_STORE).getAll())) as SessionRecord[]
  db.close()
  const weekAgo = Date.now() - 7 * 24 * 60 * 60 * 1000
  return values
    .filter((s) => s.updatedAt > weekAgo && s.status !== 'done')
    .sort((a, b) => b.updatedAt - a.updatedAt)[0]
}

export async function clearSession(roomCode: string): Promise<void> {
  const db = await openDb()
  const tx = db.transaction([SESSION_STORE, HANDLE_STORE], 'readwrite')
  const code = roomCode.toUpperCase()
  await Promise.all([
    reqToPromise(tx.objectStore(SESSION_STORE).delete(code)),
    reqToPromise(tx.objectStore(HANDLE_STORE).delete(`${code}:source`)),
    reqToPromise(tx.objectStore(HANDLE_STORE).delete(`${code}:dest`)),
  ])
  db.close()
}

export async function saveHandle(
  roomCode: string,
  kind: 'source' | 'dest',
  handle: FileSystemDirectoryHandle,
): Promise<void> {
  const db = await openDb()
  const tx = db.transaction(HANDLE_STORE, 'readwrite')
  await reqToPromise(tx.objectStore(HANDLE_STORE).put(handle, `${roomCode.toUpperCase()}:${kind}`))
  db.close()
}

export async function getHandle(
  roomCode: string,
  kind: 'source' | 'dest',
): Promise<FileSystemDirectoryHandle | undefined> {
  const db = await openDb()
  const tx = db.transaction(HANDLE_STORE, 'readonly')
  const value = await reqToPromise(tx.objectStore(HANDLE_STORE).get(`${roomCode.toUpperCase()}:${kind}`))
  db.close()
  return value as FileSystemDirectoryHandle | undefined
}

export async function ensureHandlePermission(
  handle: FileSystemDirectoryHandle,
  mode: 'read' | 'readwrite',
): Promise<boolean> {
  const current = await handle.queryPermission({ mode })
  if (current === 'granted') return true
  const next = await handle.requestPermission({ mode })
  return next === 'granted'
}
