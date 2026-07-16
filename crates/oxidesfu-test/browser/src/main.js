const parameters = new URLSearchParams(window.location.search);
const forceVp9 = parameters.get('codec') === 'vp9';
const nativePeerConnection = window.RTCPeerConnection;
const peerConnections = [];
const peerConnectionIds = new Map();
const observedDataChannels = [];
const observedSessionDescriptions = [];
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

const nativeAddTransceiver = nativePeerConnection.prototype.addTransceiver;
nativePeerConnection.prototype.addTransceiver = function (trackOrKind, init) {
  const isVideo = trackOrKind === 'video' || trackOrKind.kind === 'video';
  const videoInit = forceVp9 && isVideo
    ? {
        ...init,
        sendEncodings: (init?.sendEncodings ?? [{}]).map((encoding) => ({
          ...encoding,
          scalabilityMode: 'L3T3_KEY',
        })),
      }
    : init;
  const transceiver = nativeAddTransceiver.call(this, trackOrKind, videoInit);
  if (forceVp9 && isVideo) {
    const vp9Codecs = RTCRtpReceiver.getCapabilities('video')?.codecs
      .filter((codec) => codec.mimeType.toLowerCase() === 'video/vp9');
    if (!vp9Codecs?.length) throw new Error('Firefox does not expose a VP9 video codec capability');
    transceiver.setCodecPreferences(vp9Codecs);
  }
  return transceiver;
};

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

function observeDataChannel(peerConnection, dataChannel, origin) {
  if (observedDataChannels.some((entry) => entry.peerConnection === peerConnection && entry.dataChannel === dataChannel)) {
    return;
  }
  observedDataChannels.push({ peerConnection, dataChannel, origin });
}

function describeSdp(sdp) {
  const sections = [];
  for (const section of sdp.split(/\r?\n(?=m=)/)) {
    const lines = section.split(/\r?\n/);
    const media = lines.find((line) => line.startsWith('m='))?.slice(2).split(' ')[0];
    if (!media) continue;
    const attribute = (prefix) => lines.find((line) => line.startsWith(prefix))?.slice(prefix.length);
    sections.push({
      media,
      mid: attribute('a=mid:'),
      direction: ['sendrecv', 'sendonly', 'recvonly', 'inactive'].find((value) => lines.includes(`a=${value}`)),
      setup: attribute('a=setup:'),
      hasIceCredentials: lines.some((line) => line.startsWith('a=ice-ufrag:')) && lines.some((line) => line.startsWith('a=ice-pwd:')),
      candidateCount: lines.filter((line) => line.startsWith('a=candidate:')).length,
      hasEndOfCandidates: lines.includes('a=end-of-candidates'),
      hasSctpPort: lines.some((line) => line.startsWith('a=sctp-port:')),
    });
  }
  return sections;
}

function observeSessionDescription(peerConnection, direction, description) {
  if (!description?.sdp) return;
  observedSessionDescriptions.push({ peerConnection, direction, type: description.type, sections: describeSdp(description.sdp) });
}

window.RTCPeerConnection = class extends nativePeerConnection {
  constructor(...args) {
    super(...args);
    peerConnections.push(this);
    peerConnectionIds.set(this, `pc-${peerConnections.length}`);
    this.addEventListener('datachannel', ({ channel }) => observeDataChannel(this, channel, 'remote'));
    const recordState = () => {
      peerConnectionStateHistory.push(
        `connection=${this.connectionState},ice=${this.iceConnectionState},gathering=${this.iceGatheringState}`,
      );
    };
    this.addEventListener('connectionstatechange', recordState);
    this.addEventListener('iceconnectionstatechange', recordState);
    recordState();
  }

  createDataChannel(...args) {
    const dataChannel = super.createDataChannel(...args);
    observeDataChannel(this, dataChannel, 'local');
    return dataChannel;
  }

  async setLocalDescription(...args) {
    await super.setLocalDescription(...args);
    observeSessionDescription(this, 'local', this.localDescription);
  }

  async setRemoteDescription(...args) {
    await super.setRemoteDescription(...args);
    observeSessionDescription(this, 'remote', this.remoteDescription);
  }
};

const role = parameters.get('role');
const url = parameters.get('url');
const token = parameters.get('token');
const codec = parameters.get('codec');
const scalabilityMode = parameters.get('scalabilityMode');
const singlePeerConnection = parameters.get('singlePeerConnection') !== 'false';
const ready = document.querySelector('[data-testid="browser-harness-ready"]');
const video = document.querySelector('[data-testid="remote-video"]');

if (!role || !url || !token) {
  ready.textContent = 'missing role, url, or token';
  throw new Error('Browser harness requires role, url, and token query parameters');
}

try {
  const { LocalVideoTrack, Room, RoomEvent, Track, VideoQuality } =
    await import('livekit-client');
  const room = new Room({
    singlePeerConnection,
    ...(codec === 'vp9' && scalabilityMode === 'L3T3_KEY'
      ? {
          publishDefaults: {
            simulcast: false,
            videoCodec: 'vp9',
            scalabilityMode: 'L3T3_KEY',
          },
        }
      : {}),
  });
  let publication;
  let publishedMediaTrack;
  const receivedChatMessages = [];

  room.registerTextStreamHandler('lk.chat', async (reader) => {
    receivedChatMessages.push(await reader.readAll());
  });

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
    ready.textContent = 'creating-synthetic-video';
    const canvas = document.createElement('canvas');
    canvas.width = 1280;
    canvas.height = 720;
    const context = canvas.getContext('2d');
    if (!context) throw new Error('Could not create a canvas rendering context');

    let frame = 0;
    const drawFrame = () => {
      context.fillStyle = '#1a365d';
      context.fillRect(0, 0, canvas.width, canvas.height);
      context.fillStyle = '#f7fafc';
      context.font = 'bold 64px sans-serif';
      context.fillText(`OxideSFU frame ${frame++}`, 80, 180);
      context.fillStyle = '#63b3ed';
      context.fillRect((frame * 13) % 1000, 300, 180, 180);
    };
    drawFrame();
    window.setInterval(drawFrame, 33);
    const mediaTrack = canvas.captureStream(30).getVideoTracks()[0];
    if (!mediaTrack) throw new Error('Canvas capture did not provide a video track');
    publishedMediaTrack = mediaTrack;
    const track = new LocalVideoTrack(mediaTrack, undefined, true);
    ready.textContent = 'publishing-video';
    await room.localParticipant.publishTrack(
      track,
      codec === 'vp9' && scalabilityMode === 'L3T3_KEY'
        ? {
            simulcast: false,
            videoCodec: 'vp9',
            scalabilityMode: 'L3T3_KEY',
          }
        : { simulcast: true },
    );
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
          const codecReport = report.codecId ? stats.get(report.codecId) : undefined;
          return {
            pcId: `${pc.getConfiguration().iceServers.length}:${receiver.track.id}`,
            trackId: receiver.track.id,
            packetsReceived: report.packetsReceived ?? 0,
            framesDecoded: report.framesDecoded ?? 0,
            codec: codecReport?.mimeType?.toLowerCase() ?? 'unknown',
          };
        }
      }
    }
    throw new Error('No inbound video RTP report belongs to the rendered track');
  };

  window.oxidesfuPublisherSample = async () => {
    if (!publishedMediaTrack) throw new Error('No local video track is published');

    for (const pc of peerConnections) {
      const sender = pc.getSenders().find((candidate) => candidate.track?.id === publishedMediaTrack.id);
      if (!sender) continue;
      const stats = await sender.getStats();
      for (const report of stats.values()) {
        if (report.type !== 'outbound-rtp' || report.kind !== 'video') continue;
        const codecReport = report.codecId ? stats.get(report.codecId) : undefined;
        return {
          codec: codecReport?.mimeType?.toLowerCase() ?? 'unknown',
          requestedScalabilityMode: forceVp9 ? 'L3T3_KEY' : undefined,
        };
      }
    }
    throw new Error('No outbound video RTP report belongs to the published track');
  };

  window.oxidesfuDataChannelSample = () => observedDataChannels.map(({ peerConnection, dataChannel, origin }) => ({
    pcId: peerConnectionIds.get(peerConnection) ?? 'unknown',
    origin,
    label: dataChannel.label,
    readyState: dataChannel.readyState,
    bufferedAmount: dataChannel.bufferedAmount,
    ordered: dataChannel.ordered,
  }));
  window.oxidesfuPeerConnectionSample = () => peerConnections.map((peerConnection) => ({
    pcId: peerConnectionIds.get(peerConnection) ?? 'unknown',
    connectionState: peerConnection.connectionState,
    iceConnectionState: peerConnection.iceConnectionState,
  }));
  window.oxidesfuSessionDescriptionSample = () => observedSessionDescriptions.map(({ peerConnection, direction, type, sections }) => ({
    pcId: peerConnectionIds.get(peerConnection) ?? 'unknown',
    direction,
    type,
    sections,
  }));
  window.oxidesfuSendChatMessage = async (message) => {
    await room.localParticipant.sendText(message, { topic: 'lk.chat' });
  };
  window.oxidesfuReceivedChatMessages = () => receivedChatMessages.slice();
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
