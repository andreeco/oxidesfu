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
    oxidesfuSendChatMessage: (message: string) => Promise<void>;
    oxidesfuReceivedChatMessages: () => string[];
    oxidesfuClose: () => void;
  }
}
