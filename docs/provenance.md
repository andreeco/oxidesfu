# Source provenance

OxideSFU is an independent Rust project implementing compatibility behavior for
the LiveKit ecosystem. It does not vendor the LiveKit server source tree.

Some portions may have been developed with reference to or adapted from
Apache-2.0-licensed upstream LiveKit source at commit
`00348c1299b77aeca8678152f7d3faab76fdebbd`. This repository uses Apache-2.0
conservatively and retains relevant upstream notices in [`NOTICE`](../NOTICE).

The implementation was developed with LLM assistance, source inspection,
compatibility tests, and differential checks. A local compatibility fork was
used as audit context; it does not replace upstream LiveKit as the attribution
source. References to Pion/WebRTC behavior are not, by themselves, claims that
Pion source was copied. Where SFU-derived material is retained, the ion-sfu MIT
notice is included in [`NOTICE`](../NOTICE).

The pinned `webrtc`/`rtc` dependency is a separate WebRTC.rs-derived project
licensed under MIT/Apache-2.0. It is consumed as a Git dependency rather than
vendored into this repository; its upstream license files remain available in
that dependency source and must be preserved when redistributing that source.
This document is a general attribution record, not a legal opinion.

Future work that incorporates third-party source material must preserve the
applicable license and notice requirements.

_This is just a defensive note until it is more clear if the source code could
be published as MIT._
