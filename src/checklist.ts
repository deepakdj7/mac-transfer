import type { FileEntry, FileProgress } from './types.ts'

export function emptyChecklist(files: FileEntry[]): FileProgress[] {
  return files.map((file) => ({
    path: file.path,
    size: file.size,
    status: 'pending',
    bytes: 0,
  }))
}

export function applyInventory(
  files: FileEntry[],
  inventory: Array<{ path: string; bytes: number }>,
): FileProgress[] {
  const present = new Map(inventory.map((item) => [item.path, item.bytes]))
  return files.map((file) => {
    const bytes = present.get(file.path) ?? 0
    const exists = present.has(file.path)
    if (file.size === 0 && exists) {
      return { path: file.path, size: 0, status: 'done', bytes: 0 }
    }
    if (file.size > 0 && bytes >= file.size) {
      return { path: file.path, size: file.size, status: 'done', bytes: file.size }
    }
    if (bytes > 0) {
      return { path: file.path, size: file.size, status: 'partial', bytes }
    }
    return { path: file.path, size: file.size, status: 'pending', bytes: 0 }
  })
}

export function summarizeChecklist(list: FileProgress[]) {
  let done = 0
  let partial = 0
  let failed = 0
  let pending = 0
  let bytesDone = 0
  for (const file of list) {
    if (file.status === 'done') {
      done += 1
      bytesDone += file.size
    } else if (file.status === 'partial') {
      partial += 1
      bytesDone += file.bytes
    } else if (file.status === 'failed') {
      failed += 1
      bytesDone += file.bytes
    } else {
      pending += 1
    }
  }
  return {
    done,
    partial,
    failed,
    pending,
    remaining: partial + failed + pending,
    bytesDone,
    total: list.length,
  }
}

export function remainingFiles(list: FileProgress[], limit = 250): FileProgress[] {
  return list.filter((file) => file.status !== 'done').slice(0, limit)
}

export function destBytesFor(file: FileProgress): number {
  if (file.status === 'done') return file.size
  if (file.status === 'partial' || file.status === 'failed') return file.bytes
  return 0
}
