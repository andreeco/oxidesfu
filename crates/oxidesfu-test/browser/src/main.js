const nativePeerConnection = window.RTCPeerConnection;
const peerConnections = [];

window.RTCPeerConnection = class extends nativePeerConnection {
  constructor(...args) {
    super(...args);
    peerConnections.push(this);
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

  room.on(RoomEvent.TrackSubscribed, (track, remotePublication) => {
    if (track.kind !== Track.Kind.Video) return;
    publication = remotePublication;
    track.attach(video);
  });

  const signalUrl = url
    .replace(/^http:/, 'ws:')
    .replace(/^https:/, 'wss:');
  await room.connect(signalUrl, token);

  if (role === 'publisher') {
    const track = await createLocalVideoTrack({
      resolution: { width: 1280, height: 720 },
    });
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
  ready.textContent = `error: ${error instanceof Error ? error.message : String(error)}`;
  console.error('OxideSFU browser harness startup failed', error);
}
