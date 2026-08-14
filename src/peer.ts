import type { Role, SignalMessage } from './types.ts'
import { DATA_CHANNEL_COUNT } from './types.ts'
import type { SignalingRoom } from './signaling.ts'

const ICE_SERVERS: RTCIceServer[] = [{ urls: 'stun:stun.l.google.com:19302' }]
const CONTROL_LABEL = 'mac-transfer'

export type PeerConnection = {
  pc: RTCPeerConnection
  channel: RTCDataChannel
  dataChannels: RTCDataChannel[]
  close: () => void
}

function waitForChannelOpen(channel: RTCDataChannel): Promise<void> {
  if (channel.readyState === 'open') return Promise.resolve()
  return new Promise((resolve, reject) => {
    const onOpen = () => {
      cleanup()
      resolve()
    }
    const onError = () => {
      cleanup()
      reject(new Error('Data channel failed to open'))
    }
    const cleanup = () => {
      channel.removeEventListener('open', onOpen)
      channel.removeEventListener('error', onError)
    }
    channel.addEventListener('open', onOpen)
    channel.addEventListener('error', onError)
  })
}

function createDataChannel(pc: RTCPeerConnection, label: string, ordered: boolean): RTCDataChannel {
  const channel = pc.createDataChannel(label, { ordered })
  channel.binaryType = 'arraybuffer'
  return channel
}

export async function startPeer(
  role: Role,
  signaling: SignalingRoom,
  attachSignal: (handler: (message: SignalMessage) => void) => void,
  onStatus?: (text: string) => void,
): Promise<PeerConnection> {
  const pc = new RTCPeerConnection({
    iceServers: ICE_SERVERS,
    iceCandidatePoolSize: 2,
  })
  const pendingIce: RTCIceCandidateInit[] = []
  let remoteSet = false

  const addIce = async (candidate: RTCIceCandidateInit) => {
    if (!remoteSet) {
      pendingIce.push(candidate)
      return
    }
    try {
      await pc.addIceCandidate(candidate)
    } catch {
      // ignore stale candidates
    }
  }

  const flushIce = async () => {
    remoteSet = true
    while (pendingIce.length) {
      await addIce(pendingIce.shift()!)
    }
  }

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      signaling.publish({ type: 'ice', candidate: event.candidate.toJSON() })
    }
  }

  pc.oniceconnectionstatechange = () => {
    if (pc.iceConnectionState === 'checking') onStatus?.('Connecting over Wi-Fi…')
    if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
      onStatus?.('Peer connected')
    }
  }

  let resolveOffer: ((sdp: string) => void) | null = null
  const offerPromise = new Promise<string>((resolve) => {
    resolveOffer = resolve
  })

  attachSignal((message) => {
    if (message.type === 'ice') void addIce(message.candidate)
    if (message.type === 'offer' && role === 'receiver') resolveOffer?.(message.sdp)
    if (message.type === 'answer' && role === 'sender') {
      void (async () => {
        if (pc.remoteDescription) return
        await pc.setRemoteDescription({ type: 'answer', sdp: message.sdp })
        await flushIce()
      })()
    }
  })

  let channelsPromise: Promise<{ control: RTCDataChannel; data: RTCDataChannel[] }>
  if (role === 'sender') {
    const control = createDataChannel(pc, CONTROL_LABEL, true)
    const data = Array.from({ length: DATA_CHANNEL_COUNT }, (_, i) =>
      createDataChannel(pc, `${CONTROL_LABEL}-d${i}`, i === 0),
    )
    channelsPromise = Promise.resolve({ control, data })
    onStatus?.('Creating a direct link…')
    const offer = await pc.createOffer()
    await pc.setLocalDescription(offer)
    signaling.publish({ type: 'offer', sdp: offer.sdp ?? '' })
  } else {
    channelsPromise = new Promise((resolve, reject) => {
      const found = new Map<string, RTCDataChannel>()
      const timer = window.setTimeout(() => reject(new Error('Timed out waiting for data channel')), 30000)
      pc.ondatachannel = (event) => {
        event.channel.binaryType = 'arraybuffer'
        found.set(event.channel.label, event.channel)
        const control = found.get(CONTROL_LABEL)
        const data = Array.from({ length: DATA_CHANNEL_COUNT }, (_, i) => found.get(`${CONTROL_LABEL}-d${i}`))
        if (control && data.every((ch) => ch)) {
          window.clearTimeout(timer)
          resolve({ control, data: data as RTCDataChannel[] })
        }
      }
    })
    onStatus?.('Waiting for sender offer…')
    const sdp = await offerPromise
    await pc.setRemoteDescription({ type: 'offer', sdp })
    await flushIce()
    const answer = await pc.createAnswer()
    await pc.setLocalDescription(answer)
    signaling.publish({ type: 'answer', sdp: answer.sdp ?? '' })
  }

  const opened = await Promise.race([
    channelsPromise.then(async ({ control, data }) => {
      await Promise.all([control, ...data].map(waitForChannelOpen))
      return { control, data }
    }),
    new Promise<never>((_, reject) => {
      window.setTimeout(() => {
        reject(
          new Error(
            'Could not open a direct Wi-Fi link in 30s. Stay on the same network, keep both tabs visible, and turn off AP/client isolation if the router has it.',
          ),
        )
      }, 30000)
    }),
  ])

  return {
    pc,
    channel: opened.control,
    dataChannels: opened.data,
    close() {
      try {
        opened.control.close()
        for (const channel of opened.data) channel.close()
        pc.close()
      } catch {
        // ignore
      }
    },
  }
}
