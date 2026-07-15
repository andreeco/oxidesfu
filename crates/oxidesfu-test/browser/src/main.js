const nativePeerConnection = window.RTCPeerConnection;
const peerConnections = [];
const peerConnectionStateHistory = [];
const remoteIceCandidateHistory = [];
let descriptionError;

function observeDescriptionSetter(method) {
  const original = nativePeerConnection.prototype[method];
  nativePeerConnection.prototype[method] = async function (description) {
    try {
      return await original.call(this, description);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      descriptionError = `${method}(${description?.type ?? 'unknown'}): ${message}`;
      throw error;
    }
  };
}

observeDescriptionSetter('setLocalDescription');
observeDescriptionSetter('setRemoteDescription');

const nativeAddIceCandidate = nativePeerConnection.prototype.addIceCandidate;
nativePeerConnection.prototype.addIceCandidate = async function (candidate) {
  remoteIceCandidateHistory.push(
    `mid=${candidate?.sdpMid ?? 'null'},mline=${candidate?.sdpMLineIndex ?? 'null'}`,
  );
  try {
    return await nativeAddIceCandidate.call(this, candidate);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    descriptionError = `addIceCandidate(${candidate?.sdpMid ?? 'null'}): ${message}`;
    throw error;
  }
};

window.RTCPeerConnection = class extends nativePeerConnection {
  constructor(...args) {
    super(...args);
    peerConnections.push(this);
    const recordState = () => {
      peerConnectionStateHistory.push(
        `connection=${this.connectionState},ice=${this.iceConnectionState},gathering=${this.iceGatheringState}`,
      );
    };
    this.addEventListener('connectionstatechange', recordState);
    this.addEventListener('iceconnectionstatechange', recordState);
    recordState();
  }
};

const parameters = new URLSearchParams(window.location.search);
const role = parameters.get('role');
const url = parameters.get('url');
const token = parameters.get('token');
const ready = document.querySelector('[data-testid="browser-harness-ready"]');
const video = document.querySelector('[data-testid="remote-video"]');

if (!role || !url || !token) {
  ready.textContent = 'missing role, url, or token';
  throw new Error('Browser harness requires role, url, and token query parameters');
}

try {
  const { Room, RoomEvent, Track, VideoQuality, createLocalVideoTrack } =
    await import('livekit-client');
  const room = new Room();
    let publication;

    room.on(RoomEvent.Connected, () => {
      ready.textContent = 'connected';
    });
    room.on(RoomEvent.Disconnected, (reason) => {
      ready.textContent = `disconnected: ${reason ?? 'unknown'}`;
    });
    room.on(RoomEvent.TrackSubscribed, (track, remotePublication) => {
    if (track.kind !== Track.Kind.Video) return;
    publication = remotePublication;
    track.attach(video);
  });

  const signalUrl = url
    .replace(/^http:/, 'ws:')
    .replace(/^https:/, 'wss:');
  ready.textContent = 'connecting';
  await room.connect(signalUrl, token);

  if (role === 'publisher') {
    ready.textContent = 'acquiring-video';
    const track = await createLocalVideoTrack({
      resolution: { width: 1280, height: 720 },
    });
    ready.textContent = 'publishing-video';
    await room.localParticipant.publishTrack(track, { simulcast: true });
  }

  window.oxidesfuSetQuality = (quality) => {
    if (!publication) throw new Error('No remote video publication is attached');
    publication.setVideoQuality(quality === 'low' ? VideoQuality.Low : VideoQuality.High);
  };

  window.oxidesfuReceiverSample = async () => {
    const track = video.srcObject?.getVideoTracks()[0];
    if (!track) throw new Error('Rendered video element has no active video track');

    for (const pc of peerConnections) {
      const receiver = pc.getReceivers().find((candidate) => candidate.track?.id === track.id);
      if (!receiver) continue;
      const stats = await receiver.getStats();
      for (const report of stats.values()) {
        if (report.type === 'inbound-rtp' && report.kind === 'video') {
          return {
            pcId: `${pc.getConfiguration().iceServers.length}:${receiver.track.id}`,
            trackId: receiver.track.id,
            packetsReceived: report.packetsReceived ?? 0,
            framesDecoded: report.framesDecoded ?? 0,
          };
        }
      }
    }
    throw new Error('No inbound video RTP report belongs to the rendered track');
  };

  window.oxidesfuClose = () => room.disconnect();
  ready.textContent = 'ready';
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  const detail = descriptionError ?? message;
  const states = peerConnectionStateHistory.join(' -> ');
  const candidates = remoteIceCandidateHistory.join(' -> ');
  ready.textContent = `error: ${detail}${states ? ` [${states}]` : ''}${candidates ? ` [remote candidates: ${candidates}]` : ''}`;
  console.error('OxideSFU browser harness startup failed', error, {
    descriptionError,
    peerConnectionStateHistory,
    remoteIceCandidateHistory,
  });
}
