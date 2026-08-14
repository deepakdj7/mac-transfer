export type Role = 'sender' | 'receiver'

export type FileEntry = {
  path: string
  size: number
}

export type Manifest = {
  folderName: string
  files: FileEntry[]
  totalBytes: number
  verifyChecksums: boolean
}

export type ControlMessage =
  | { type: 'manifest'; manifest: Manifest }
  | { type: 'file-start'; index: number; path: string; size: number; offset: number }
  | { type: 'file-end'; index: number; sha256?: string }
  | { type: 'done' }
  | { type: 'pause' }
  | { type: 'resume' }
  | { type: 'cancel'; reason?: string }
  | { type: 'ready'; resumeFrom?: { index: number; offset: number } }
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
}

export const CHUNK_SIZE = 64 * 1024
export const BUFFER_HIGH = 2 * 1024 * 1024
export const BUFFER_LOW = 512 * 1024
