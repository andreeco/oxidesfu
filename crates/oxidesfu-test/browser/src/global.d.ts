export {};

declare global {
  interface Window {
    oxidesfuSetQuality: (quality: 'high' | 'low') => void;
    oxidesfuReceiverSample: () => Promise<{
      pcId: string;
      trackId: string;
      packetsReceived: number;
      framesDecoded: number;
    }>;
    oxidesfuClose: () => void;
  }
}
