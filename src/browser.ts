export function hasDirectoryPicker(): boolean {
  return typeof window !== 'undefined' && 'showDirectoryPicker' in window
}

export function isSecureContextOk(): boolean {
  return window.isSecureContext || location.hostname === 'localhost' || location.hostname === '127.0.0.1'
}

export function browserLabel(): string {
  const ua = navigator.userAgent
  if (/Edg\//.test(ua)) return 'Edge'
  if (/Chrome\//.test(ua) && !/Edg\//.test(ua)) return 'Chrome'
  if (/Safari\//.test(ua) && !/Chrome\//.test(ua)) return 'Safari'
  if (/Firefox\//.test(ua)) return 'Firefox'
  return 'this browser'
}

export function supportMessage(): string | null {
  if (!isSecureContextOk()) {
    return 'This app needs HTTPS (or localhost). Open it from GitHub Pages, not a raw file.'
  }
  if (!hasDirectoryPicker()) {
    return `${browserLabel()} cannot stream 100 GB folders to disk. Use Chrome or Edge on both Macs.`
  }
  return null
}
