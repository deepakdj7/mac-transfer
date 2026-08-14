import type { FileEntry } from './types.ts'

export type ScannedFile = FileEntry & {
  handle: FileSystemFileHandle
}

export type ScanProgress = {
  fileCount: number
  totalBytes: number
  folderName: string
}

const SKIP_NAMES = new Set(['.DS_Store', 'Thumbs.db', 'desktop.ini'])

export async function scanFolder(
  root: FileSystemDirectoryHandle,
  onProgress?: (info: ScanProgress) => void,
): Promise<ScannedFile[]> {
  const files: ScannedFile[] = []
  let totalBytes = 0
  let visited = 0

  async function walk(dir: FileSystemDirectoryHandle, prefix: string): Promise<void> {
    for await (const [name, handle] of dir.entries()) {
      if (SKIP_NAMES.has(name)) continue
      if (handle.kind === 'directory') {
        await walk(handle, `${prefix}${name}/`)
        continue
      }
      try {
        const file = await handle.getFile()
        files.push({ path: `${prefix}${name}`, size: file.size, handle })
        totalBytes += file.size
        visited += 1
        if (visited % 40 === 0) {
          onProgress?.({ fileCount: files.length, totalBytes, folderName: root.name })
          await new Promise((r) => setTimeout(r, 0))
        }
      } catch {
        // Unreadable file (permissions, broken alias). Skip it.
      }
    }
  }

  await walk(root, '')
  onProgress?.({ fileCount: files.length, totalBytes, folderName: root.name })
  return files
}

export async function openWritable(
  root: FileSystemDirectoryHandle,
  path: string,
  offset: number,
): Promise<FileSystemWritableFileStream> {
  const parts = path.split('/').filter(Boolean)
  if (parts.length === 0) throw new Error('Invalid file path')

  let dir = root
  for (const part of parts.slice(0, -1)) {
    dir = await dir.getDirectoryHandle(part, { create: true })
  }

  const fileHandle = await dir.getFileHandle(parts[parts.length - 1], { create: true })
  const writable = await fileHandle.createWritable({ keepExistingData: offset > 0 })
  if (offset > 0) {
    await writable.seek(offset)
  }
  return writable
}

export async function inventoryFolder(root: FileSystemDirectoryHandle): Promise<Map<string, number>> {
  const files = await scanFolder(root)
  return new Map(files.map((file) => [file.path, file.size]))
}

export function summarizeScan(files: ScannedFile[]): { tinyFiles: number; hugeFiles: number } {
  let tinyFiles = 0
  let hugeFiles = 0
  for (const file of files) {
    if (file.size < 4096) tinyFiles += 1
    if (file.size >= 10 * 1024 * 1024 * 1024) hugeFiles += 1
  }
  return { tinyFiles, hugeFiles }
}
