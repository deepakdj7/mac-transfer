# Mac Transfer

A static web app that moves folders (including ~100 GB) from one Mac to another on the **same Wi-Fi**. The site can live on GitHub Pages. **File bytes never go through GitHub.** They stream disk-to-disk over a WebRTC data channel.

Live URL after Pages is enabled: https://deepakdj7.github.io/mac-transfer/

## What you need

- Two Macs on the same Wi-Fi network
- [Chrome](https://www.google.com/chrome/) or [Edge](https://www.microsoft.com/edge) on both (Safari cannot stream huge folders to disk)
- Enough free space on the receiving Mac
- Both laptops awake, with this tab left visible

## How to use

1. On the sending Mac, open the site → **Send a folder** → pick the folder → **Create room**.
2. On the receiving Mac, open the site → **Receive a folder** → type the room code (or open the shared join link) → pick a destination folder.
3. Keep both tabs open until it finishes. Pause/resume is available. If a tab crashes, reopen the site and use **Resume**.

Typical time for 100 GB on a decent home LAN is about 20–90 minutes.

## How it works

1. GitHub Pages serves only the HTML/JS UI.
2. A room code is exchanged through a public MQTT broker (a few kilobytes of WebRTC handshake).
3. The browsers open a direct LAN connection. Folder data does not go through the broker or GitHub.
4. Chrome’s File System Access API reads and writes files in chunks so RAM stays flat.

If the two Macs cannot connect, the router may have **AP / client isolation** enabled (common on guest networks). Use the main LAN SSID.

## Local development

```bash
npm install
npm run dev
```

Open the printed localhost URL in Chrome. You can test two tabs on one Mac; for a real 100 GB run, use two machines.

```bash
npm run build
npm run preview
```

## Deploy

Pushing to `main` builds the app and deploys it with GitHub Pages (see `.github/workflows/deploy.yml`).

One-time repo setting: **Settings → Pages → Source → GitHub Actions**.
