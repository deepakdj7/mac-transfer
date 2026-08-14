export type Role = 'sender' | 'receiver'

export type FileEntry = {
  path: string
  size: number
}

export type FileStatus = 'pending' | 'partial' | 'done' | 'failed'

export type FileProgress = {
  path: string
  size: number
  status: FileStatus
  bytes: number
  error?: string
}

export type Manifest = {
  folderName: string
  files: FileEntry[]
  totalBytes: number
  verifyChecksums: boolean
}

export type ControlMessage =
  | { type: 'manifest'; manifest: Manifest }
  | { type: 'manifest-start'; folderName: string; totalBytes: number; verifyChecksums: boolean; count: number }
  | { type: 'manifest-part'; files: FileEntry[] }
  | { type: 'manifest-end' }
  | { type: 'file-start'; index: number; path: string; size: number; offset: number }
  | { type: 'file-end'; index: number; sha256?: string }
  | { type: 'file-ack'; index: number }
  | { type: 'done' }
  | { type: 'pause' }
  | { type: 'resume' }
  | { type: 'cancel'; reason?: string }
  | { type: 'ready'; resumeFrom?: { index: number; offset: number } }
  | { type: 'inventory'; files: Array<{ path: string; bytes: number }> }
  | { type: 'inventory-start'; count: number }
  | { type: 'inventory-part'; files: Array<{ path: string; bytes: number }> }
  | { type: 'inventory-end' }
  | { type: 'error'; message: string }

export type SignalMessage =
  | { type: 'hello'; role: Role; id: string }
  | { type: 'offer'; sdp: string }
  | { type: 'answer'; sdp: string }
  | { type: 'ice'; candidate: RTCIceCandidateInit }

export type TransferProgress = {
  fileIndex: number
  filePath: string
  fileBytes: number
  fileSize: number
  bytesDone: number
  totalBytes: number
  speed: number
  filesDone: number
  filesTotal: number
}

export type SessionRecord = {
  roomCode: string
  role: Role
  manifest?: Manifest
  fileIndex: number
  fileOffset: number
  bytesDone: number
  verifyChecksums: boolean
  updatedAt: number
  status?: 'active' | 'failed' | 'done'
  lastError?: string
  files?: FileProgress[]
}

export const CHUNK_SIZE = 48 * 1024
export const READ_AHEAD = 4 * 1024 * 1024
export const BUFFER_HIGH = 16 * 1024 * 1024
export const BUFFER_LOW = 8 * 1024 * 1024
export const DATA_CHANNEL_COUNT = 4
export const WRITE_BACKPRESSURE = 32 * 1024 * 1024
