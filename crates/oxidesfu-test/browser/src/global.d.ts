export {};

declare global {
  interface Window {
    oxidesfuSetQuality: (quality: 'high' | 'low') => void;
    oxidesfuReceiverSample: () => Promise<{
      pcId: string;
      trackId: string;
      packetsReceived: number;
      framesDecoded: number;
      codec: string;
    }>;
    oxidesfuPublisherSample: () => Promise<{
      codec: string;
      requestedScalabilityMode?: 'L3T3_KEY';
    }>;
    oxidesfuDataChannelSample: () => Array<{
      pcId: string;
      origin: 'local' | 'remote';
      label: string;
      readyState: RTCDataChannelState;
      bufferedAmount: number;
      ordered: boolean;
    }>;
    oxidesfuPeerConnectionSample: () => Promise<Array<{
      pcId: string;
      connectionState: RTCPeerConnectionState;
      iceConnectionState: RTCIceConnectionState;
      selectedCandidatePair?: {
        state: string;
        localProtocol?: string;
        remoteProtocol?: string;
        localCandidateType?: string;
        remoteCandidateType?: string;
      };
    }>>;
    oxidesfuSessionDescriptionSample: () => Array<{
      pcId: string;
      direction: 'local' | 'remote';
      type: RTCSdpType | null;
      sections: Array<{
        media: string;
        mid?: string;
        direction?: string;
        setup?: string;
        hasIceCredentials: boolean;
        candidateCount: number;
        hasEndOfCandidates: boolean;
        hasSctpPort: boolean;
      }>;
    }>;
    oxidesfuSendChatMessage: (message: string) => Promise<void>;
    oxidesfuReceivedChatMessages: () => string[];
    oxidesfuClose: () => void;
  }
}
