use std::{future::Future, pin::Pin};

use super::{
    ForwardTrackKey, KeyFrameRequestKind, MappedSenderReport, MediaFeedbackSummary,
    RecommendedVideoQuality, RtpForwardingStore,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RtcpForwardAction {
    RetransmitSequence(u16),
    KeyFrameRequest {
        kind: KeyFrameRequestKind,
        media_ssrc: u32,
    },
    SenderReport {
        report: rtc::rtcp::sender_report::SenderReport,
    },
    ReceiverReportObserved {
        ssrc: u32,
        max_fraction_lost: u8,
        report_count: u16,
    },
    TransportWideCcObserved {
        media_ssrc: u32,
        packet_status_count: u16,
    },
}

pub(crate) fn derive_rtcp_forward_actions(
    packets: &[Box<dyn rtc::rtcp::Packet>],
) -> Vec<RtcpForwardAction> {
    let mut actions = Vec::new();

    for packet in packets {
        if let Some(nack) = packet.as_any().downcast_ref::<
            rtc::rtcp::transport_feedbacks::transport_layer_nack::TransportLayerNack,
        >() {
            for nack_pair in &nack.nacks {
                for sequence_number in nack_pair.packet_list() {
                    actions.push(RtcpForwardAction::RetransmitSequence(sequence_number));
                }
            }
            continue;
        }

        if let Some(pli) = packet.as_any().downcast_ref::<
            rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
        >() {
            actions.push(RtcpForwardAction::KeyFrameRequest {
                kind: KeyFrameRequestKind::Pli,
                media_ssrc: pli.media_ssrc,
            });
            continue;
        }

        if let Some(fir) = packet
            .as_any()
            .downcast_ref::<rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest>(
        ) {
            actions.push(RtcpForwardAction::KeyFrameRequest {
                kind: KeyFrameRequestKind::Fir,
                media_ssrc: fir.media_ssrc,
            });
            continue;
        }

        if let Some(sender_report) = packet
            .as_any()
            .downcast_ref::<rtc::rtcp::sender_report::SenderReport>()
        {
            actions.push(RtcpForwardAction::SenderReport {
                report: sender_report.clone(),
            });
            continue;
        }

        if let Some(receiver_report) = packet
            .as_any()
            .downcast_ref::<rtc::rtcp::receiver_report::ReceiverReport>()
        {
            let max_fraction_lost = receiver_report
                .reports
                .iter()
                .map(|report| report.fraction_lost)
                .max()
                .unwrap_or_default();
            let report_count = receiver_report.reports.len() as u16;
            actions.push(RtcpForwardAction::ReceiverReportObserved {
                ssrc: receiver_report.ssrc,
                max_fraction_lost,
                report_count,
            });
            continue;
        }

        if let Some(twcc) = packet
            .as_any()
            .downcast_ref::<rtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc>(
        ) {
            actions.push(RtcpForwardAction::TransportWideCcObserved {
                media_ssrc: twcc.media_ssrc,
                packet_status_count: twcc.packet_status_count,
            });
        }
    }

    actions
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyframeFeedbackRequest {
    pub(crate) kind: KeyFrameRequestKind,
    pub(crate) media_ssrc: u32,
    pub(crate) fir_sequence_number: Option<u8>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RtcpExecutionPlan {
    pub(crate) retransmit_packets: Vec<rtc::rtp::Packet>,
    pub(crate) keyframe_requests: Vec<KeyframeFeedbackRequest>,
    pub(crate) rewritten_sender_reports: Vec<rtc::rtcp::sender_report::SenderReport>,
    pub(crate) media_feedback: MediaFeedbackSummary,
    pub(crate) recommended_video_quality: Option<RecommendedVideoQuality>,
}

pub(crate) struct RtcpOutboundEffects {
    pub(crate) retransmit_packets: Vec<rtc::rtp::Packet>,
    pub(crate) feedback_packets: Vec<Box<dyn rtc::rtcp::Packet>>,
    pub(crate) sender_report_packets: Vec<Box<dyn rtc::rtcp::Packet>>,
    pub(crate) recommended_video_quality: Option<RecommendedVideoQuality>,
}

pub(crate) fn build_rtcp_execution_plan(
    key: &ForwardTrackKey,
    actions: &[RtcpForwardAction],
    rtp_forwarding: &RtpForwardingStore,
    now_millis: i64,
) -> RtcpExecutionPlan {
    const KEYFRAME_REQUEST_MIN_GAP_MILLIS: u64 = 300;

    let now_millis = u64::try_from(now_millis).unwrap_or_default();
    let mut plan = RtcpExecutionPlan::default();

    for action in actions {
        match *action {
            RtcpForwardAction::RetransmitSequence(outgoing_sequence_number) => {
                if let Some(packet) =
                    rtp_forwarding.get_retransmission_packet(key, outgoing_sequence_number)
                {
                    plan.retransmit_packets.push(packet);
                }
            }
            RtcpForwardAction::KeyFrameRequest { kind, media_ssrc } => {
                if rtp_forwarding.should_forward_keyframe_request(
                    key,
                    kind,
                    now_millis,
                    KEYFRAME_REQUEST_MIN_GAP_MILLIS,
                ) {
                    let fir_sequence_number = if kind == KeyFrameRequestKind::Fir {
                        Some(rtp_forwarding.next_fir_sequence_number(key, media_ssrc))
                    } else {
                        None
                    };
                    plan.keyframe_requests.push(KeyframeFeedbackRequest {
                        kind,
                        media_ssrc,
                        fir_sequence_number,
                    });
                }
            }
            RtcpForwardAction::SenderReport { ref report } => {
                let mapped = rtp_forwarding.map_sender_report(key, report.ssrc, report.rtp_time);
                let rewritten = rewrite_sender_report_packet(report, mapped);
                plan.rewritten_sender_reports.push(rewritten);
            }
            RtcpForwardAction::ReceiverReportObserved {
                ssrc,
                max_fraction_lost,
                report_count,
            } => {
                rtp_forwarding.observe_receiver_report(
                    key,
                    now_millis,
                    ssrc,
                    max_fraction_lost,
                    report_count,
                );
            }
            RtcpForwardAction::TransportWideCcObserved {
                media_ssrc,
                packet_status_count,
            } => {
                rtp_forwarding.observe_transport_wide_cc(
                    key,
                    now_millis,
                    media_ssrc,
                    packet_status_count,
                );
            }
        }
    }

    plan.media_feedback = rtp_forwarding.media_feedback_summary(key, now_millis);
    plan.recommended_video_quality = rtp_forwarding.recommend_video_quality(key, now_millis);

    plan
}

pub(crate) fn build_rtcp_outbound_effects(
    key: &ForwardTrackKey,
    actions: &[RtcpForwardAction],
    rtp_forwarding: &RtpForwardingStore,
    now_millis: i64,
) -> RtcpOutboundEffects {
    let plan = build_rtcp_execution_plan(key, actions, rtp_forwarding, now_millis);
    let _media_feedback = plan.media_feedback;
    let feedback_packets = plan
        .keyframe_requests
        .iter()
        .map(build_keyframe_feedback_packet)
        .collect();
    let sender_report_packets = plan
        .rewritten_sender_reports
        .into_iter()
        .map(|report| Box::new(report) as Box<dyn rtc::rtcp::Packet>)
        .collect();

    RtcpOutboundEffects {
        retransmit_packets: plan.retransmit_packets,
        feedback_packets,
        sender_report_packets,
        recommended_video_quality: plan.recommended_video_quality,
    }
}

pub(crate) trait RtpRetransmitSink {
    fn send_rtp<'a>(
        &'a self,
        packet: rtc::rtp::Packet,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(crate) trait RtcpFeedbackSink {
    fn send_feedback_rtcp<'a>(
        &'a self,
        packet: Box<dyn rtc::rtcp::Packet>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(crate) trait SenderReportSink {
    fn send_sender_report_rtcp<'a>(
        &'a self,
        packet: Box<dyn rtc::rtcp::Packet>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(crate) struct LocalForwardTrackRtpSink {
    pub(crate) track: oxidesfu_rtc::LocalRtpTrack,
}

impl RtpRetransmitSink for LocalForwardTrackRtpSink {
    fn send_rtp<'a>(
        &'a self,
        packet: rtc::rtp::Packet,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let _ = self.track.write_rtp(packet).await;
        })
    }
}

pub(crate) struct RemoteTrackFeedbackSink {
    pub(crate) track: oxidesfu_rtc::RemoteTrack,
}

impl RtcpFeedbackSink for RemoteTrackFeedbackSink {
    fn send_feedback_rtcp<'a>(
        &'a self,
        packet: Box<dyn rtc::rtcp::Packet>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let _ = self.track.write_rtcp_packets(vec![packet]).await;
        })
    }
}

pub(crate) struct LocalForwardTrackSenderReportSink {
    pub(crate) track: oxidesfu_rtc::LocalRtpTrack,
}

impl SenderReportSink for LocalForwardTrackSenderReportSink {
    fn send_sender_report_rtcp<'a>(
        &'a self,
        packet: Box<dyn rtc::rtcp::Packet>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let _ = self.track.write_rtcp_packets(vec![packet]).await;
        })
    }
}

pub(crate) async fn execute_rtcp_outbound_effects(
    effects: RtcpOutboundEffects,
    rtp_sink: &impl RtpRetransmitSink,
    feedback_sink: &impl RtcpFeedbackSink,
    sender_report_sink: &impl SenderReportSink,
) {
    for packet in effects.retransmit_packets {
        rtp_sink.send_rtp(packet).await;
    }

    for packet in effects.feedback_packets {
        feedback_sink.send_feedback_rtcp(packet).await;
    }

    for packet in effects.sender_report_packets {
        sender_report_sink.send_sender_report_rtcp(packet).await;
    }
}

pub(crate) fn rewrite_sender_report_packet(
    original: &rtc::rtcp::sender_report::SenderReport,
    mapped: MappedSenderReport,
) -> rtc::rtcp::sender_report::SenderReport {
    let mut rewritten = original.clone();
    rewritten.ssrc = mapped.ssrc;
    rewritten.rtp_time = mapped.rtp_timestamp;
    rewritten
}

pub(crate) fn build_keyframe_feedback_packet(
    request: &KeyframeFeedbackRequest,
) -> Box<dyn rtc::rtcp::Packet> {
    match request.kind {
        KeyFrameRequestKind::Pli => Box::new(
            rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                sender_ssrc: 0,
                media_ssrc: request.media_ssrc,
            },
        ),
        KeyFrameRequestKind::Fir => Box::new(
            rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest {
                sender_ssrc: 0,
                media_ssrc: request.media_ssrc,
                fir: vec![rtc::rtcp::payload_feedbacks::full_intra_request::FirEntry {
                    ssrc: request.media_ssrc,
                    sequence_number: request.fir_sequence_number.unwrap_or_default(),
                }],
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ForwardTrackKey {
        (
            "room-a".to_string(),
            "publisher-a".to_string(),
            "track-a".to_string(),
            "subscriber-a".to_string(),
        )
    }

    #[test]
    fn derive_rtcp_forward_actions_includes_receiver_report_and_twcc_observation() {
        let rr: Box<dyn rtc::rtcp::Packet> = Box::new(rtc::rtcp::receiver_report::ReceiverReport {
            ssrc: 77,
            reports: vec![rtc::rtcp::reception_report::ReceptionReport {
                fraction_lost: 69,
                ..Default::default()
            }],
            ..Default::default()
        });
        let twcc: Box<dyn rtc::rtcp::Packet> = Box::new(
            rtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc {
                media_ssrc: 99,
                packet_status_count: 4,
                ..Default::default()
            },
        );

        let actions = derive_rtcp_forward_actions(&[rr, twcc]);
        assert!(actions.iter().any(|action| matches!(
            action,
            RtcpForwardAction::ReceiverReportObserved {
                ssrc: 77,
                max_fraction_lost: 69,
                report_count: 1,
            }
        )));
        assert!(actions.iter().any(|action| matches!(
            action,
            RtcpForwardAction::TransportWideCcObserved {
                media_ssrc: 99,
                packet_status_count: 4
            }
        )));
    }

    #[test]
    fn build_rtcp_execution_plan_rewrites_sender_report_ssrc_and_timestamp() {
        let store = RtpForwardingStore::default();
        let key = key();
        let original = rtc::rtcp::sender_report::SenderReport {
            ssrc: 111,
            rtp_time: 90_000,
            ..Default::default()
        };
        let actions = vec![RtcpForwardAction::SenderReport {
            report: original.clone(),
        }];

        let plan = build_rtcp_execution_plan(&key, &actions, &store, 1);
        assert_eq!(plan.rewritten_sender_reports.len(), 1);
        assert_eq!(plan.rewritten_sender_reports[0].ssrc, 111);
        assert_eq!(plan.rewritten_sender_reports[0].rtp_time, 90_000);
    }

    #[test]
    fn build_rtcp_execution_plan_throttles_keyframe_requests_within_min_gap() {
        let store = RtpForwardingStore::default();
        let key = key();
        let actions = vec![RtcpForwardAction::KeyFrameRequest {
            kind: KeyFrameRequestKind::Pli,
            media_ssrc: 333,
        }];

        let first = build_rtcp_execution_plan(&key, &actions, &store, 1_000);
        let second = build_rtcp_execution_plan(&key, &actions, &store, 1_100);
        let third = build_rtcp_execution_plan(&key, &actions, &store, 1_350);

        assert_eq!(first.keyframe_requests.len(), 1);
        assert!(second.keyframe_requests.is_empty());
        assert_eq!(third.keyframe_requests.len(), 1);
    }

    #[test]
    fn build_rtcp_execution_plan_populates_media_feedback_summary_from_rr_and_twcc() {
        let store = RtpForwardingStore::default();
        let key = key();
        let actions = vec![
            RtcpForwardAction::ReceiverReportObserved {
                ssrc: 55,
                max_fraction_lost: 80,
                report_count: 1,
            },
            RtcpForwardAction::ReceiverReportObserved {
                ssrc: 55,
                max_fraction_lost: 90,
                report_count: 1,
            },
            RtcpForwardAction::TransportWideCcObserved {
                media_ssrc: 66,
                packet_status_count: 9,
            },
        ];

        let plan = build_rtcp_execution_plan(&key, &actions, &store, 33_000);
        assert_eq!(plan.media_feedback.last_rr_ssrc, Some(55));
        assert_eq!(plan.media_feedback.rr_report_count, 2);
        assert_eq!(plan.media_feedback.rr_max_fraction_lost, 90);
        assert_eq!(plan.media_feedback.last_twcc_media_ssrc, Some(66));
        assert_eq!(plan.media_feedback.twcc_packet_status_count, 9);
        assert!(plan.media_feedback.is_degraded);
        assert_eq!(
            plan.recommended_video_quality,
            Some(RecommendedVideoQuality::Low)
        );
    }
}
