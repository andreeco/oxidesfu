#[cfg(test)]
mod tests {
    use rtc::rtp::extension::dependency_descriptor_extension::DependencyDescriptorParser;

    fn decode_hex(input: &str) -> Vec<u8> {
        assert!(input.len().is_multiple_of(2), "hex length must be even");
        let mut out = Vec::with_capacity(input.len() / 2);
        let bytes = input.as_bytes();
        for i in (0..bytes.len()).step_by(2) {
            let h = bytes[i] as char;
            let l = bytes[i + 1] as char;
            let hi = h.to_digit(16).expect("invalid hex high nibble") as u8;
            let lo = l.to_digit(16).expect("invalid hex low nibble") as u8;
            out.push((hi << 4) | lo);
        }
        out
    }

    // Upstream: livekit/pkg/sfu/rtpextension/dependencydescriptor/dependencydescriptorextension_test.go::TestDependencyDescriptorUnmarshal
    #[test]
    fn dependency_descriptor_unmarshal_matches_upstream_contract() {
        let hexes = [
            "c1017280081485214eafffaaaa863cf0430c10c302afc0aaa0063c00430010c002a000a80006000040001d954926e082b04a0941b820ac1282503157f974000ca864330e222222eca8655304224230eca877530077004200ef008601df010d",
            "86017340fc",
            "46017340fc",
            "c3017540fc",
            "88017640fc",
            "48017640fc",
            "c2017840fc",
            "c1017280081485214eafffaaaa863cf0430c10c302afc0aaa0063c00430010c002a000a80006000040001d954926e082b04a0941b820ac1282503157f974000ca864330e222222eca8655304224230eca877530077004200ef008601df010d",
            "860173",
            "460173",
            "8b0174",
            "0b0174",
            "0b0174",
            "c30175",
        ];

        let mut parser = DependencyDescriptorParser::default();
        for (i, hex) in hexes.iter().enumerate() {
            let payload = decode_hex(hex);
            let parsed = parser.parse_layer_ids(&payload);
            assert!(
                parsed.is_some(),
                "expected DD parser to decode capture payload at index {i}"
            );
        }
    }
}
