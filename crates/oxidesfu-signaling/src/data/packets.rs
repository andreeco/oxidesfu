const DATA_TRACK_HEADER_LENGTH: usize = 12;
const DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH: usize = 2;
const DATA_TRACK_EXTENSION_ID_LENGTH: usize = 1;
const DATA_TRACK_EXTENSION_SIZE_LENGTH: usize = 1;

const DATA_TRACK_VERSION_SHIFT: u8 = 5;
const DATA_TRACK_VERSION_MASK: u8 = 0b111;
const DATA_TRACK_START_OF_FRAME_SHIFT: u8 = 4;
const DATA_TRACK_FINAL_OF_FRAME_SHIFT: u8 = 3;
const DATA_TRACK_EXTENSIONS_SHIFT: u8 = 2;

const DATA_TRACK_EXTENSION_ID_PARTICIPANT_SID: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DataTrackPacketError {
    HeaderSizeInsufficient {
        actual: usize,
        min: usize,
    },
    BufferSizeInsufficient {
        actual: usize,
        min: usize,
    },
    ExtensionSizeInsufficient {
        remaining: usize,
        available: usize,
        needed: usize,
    },
    ExtensionNotFound {
        id: u8,
    },
    ExtensionSizeTooBig {
        size: usize,
    },
    InvalidExtensionId {
        expected: u8,
        actual: u8,
    },
    EmptyExtensionData,
    InvalidUtf8ParticipantSid,
    ParticipantSidTooLong {
        len: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DataTrackExtension {
    pub(crate) id: u8,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct DataTrackHeader {
    pub(crate) version: u8,
    pub(crate) is_start_of_frame: bool,
    pub(crate) is_final_of_frame: bool,
    pub(crate) has_extensions: bool,
    pub(crate) handle: u16,
    pub(crate) sequence_number: u16,
    pub(crate) frame_number: u16,
    pub(crate) timestamp: u32,
    pub(crate) extensions: Vec<DataTrackExtension>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct DataTrackPacket {
    pub(crate) header: DataTrackHeader,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DataTrackExtensionParticipantSid {
    participant_sid: String,
}

impl DataTrackHeader {
    pub(crate) fn unmarshal(buf: &[u8]) -> Result<(Self, usize), DataTrackPacketError> {
        if buf.len() < DATA_TRACK_HEADER_LENGTH {
            return Err(DataTrackPacketError::HeaderSizeInsufficient {
                actual: buf.len(),
                min: DATA_TRACK_HEADER_LENGTH,
            });
        }

        let mut header = Self {
            version: (buf[0] >> DATA_TRACK_VERSION_SHIFT) & DATA_TRACK_VERSION_MASK,
            is_start_of_frame: ((buf[0] >> DATA_TRACK_START_OF_FRAME_SHIFT) & 0b1) != 0,
            is_final_of_frame: ((buf[0] >> DATA_TRACK_FINAL_OF_FRAME_SHIFT) & 0b1) != 0,
            has_extensions: ((buf[0] >> DATA_TRACK_EXTENSIONS_SHIFT) & 0b1) != 0,
            handle: u16::from_be_bytes([buf[2], buf[3]]),
            sequence_number: u16::from_be_bytes([buf[4], buf[5]]),
            frame_number: u16::from_be_bytes([buf[6], buf[7]]),
            timestamp: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
            extensions: Vec::new(),
        };

        let mut header_size = DATA_TRACK_HEADER_LENGTH;
        if header.has_extensions {
            if buf.len() < DATA_TRACK_HEADER_LENGTH + DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH {
                return Err(DataTrackPacketError::HeaderSizeInsufficient {
                    actual: buf.len(),
                    min: DATA_TRACK_HEADER_LENGTH + DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH,
                });
            }

            let extensions_area_size = (u16::from_be_bytes([buf[12], buf[13]]) as usize + 1) * 4
                - DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH;
            header_size += DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH;

            let mut remaining = extensions_area_size;
            let mut idx = DATA_TRACK_HEADER_LENGTH + DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH;

            while remaining != 0 {
                if idx >= buf.len() || remaining < DATA_TRACK_EXTENSION_ID_LENGTH {
                    return Err(DataTrackPacketError::ExtensionSizeInsufficient {
                        remaining,
                        available: buf.len().saturating_sub(idx),
                        needed: DATA_TRACK_EXTENSION_ID_LENGTH,
                    });
                }

                let id = buf[idx];
                if id == 0 {
                    header_size += remaining;
                    break;
                }

                if idx + 1 >= buf.len()
                    || remaining < DATA_TRACK_EXTENSION_ID_LENGTH + DATA_TRACK_EXTENSION_SIZE_LENGTH
                {
                    return Err(DataTrackPacketError::ExtensionSizeInsufficient {
                        remaining,
                        available: buf.len().saturating_sub(idx),
                        needed: DATA_TRACK_EXTENSION_ID_LENGTH + DATA_TRACK_EXTENSION_SIZE_LENGTH,
                    });
                }

                let extension_size = buf[idx + 1] as usize;
                remaining -= DATA_TRACK_EXTENSION_ID_LENGTH + DATA_TRACK_EXTENSION_SIZE_LENGTH;
                idx += DATA_TRACK_EXTENSION_ID_LENGTH + DATA_TRACK_EXTENSION_SIZE_LENGTH;
                header_size += DATA_TRACK_EXTENSION_ID_LENGTH + DATA_TRACK_EXTENSION_SIZE_LENGTH;

                if idx + extension_size > buf.len() || remaining < extension_size {
                    return Err(DataTrackPacketError::ExtensionSizeInsufficient {
                        remaining,
                        available: buf.len().saturating_sub(idx),
                        needed: extension_size,
                    });
                }

                header.extensions.push(DataTrackExtension {
                    id,
                    data: buf[idx..idx + extension_size].to_vec(),
                });
                remaining -= extension_size;
                idx += extension_size;
                header_size += extension_size;
            }
        }

        Ok((header, header_size))
    }

    pub(crate) fn marshal_size(&self) -> usize {
        let extension_payload_size = self
            .extensions
            .iter()
            .map(|extension| {
                DATA_TRACK_EXTENSION_ID_LENGTH
                    + DATA_TRACK_EXTENSION_SIZE_LENGTH
                    + extension.data.len()
            })
            .sum::<usize>();

        let has_extensions = self.has_extensions || !self.extensions.is_empty();
        if !has_extensions {
            return DATA_TRACK_HEADER_LENGTH;
        }

        let extension_block_size = DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH + extension_payload_size;
        let padded_extension_block_size = extension_block_size.div_ceil(4) * 4;

        DATA_TRACK_HEADER_LENGTH + padded_extension_block_size
    }

    pub(crate) fn marshal_to(&self, buf: &mut [u8]) -> Result<usize, DataTrackPacketError> {
        let required = self.marshal_size();
        if buf.len() < required {
            return Err(DataTrackPacketError::HeaderSizeInsufficient {
                actual: buf.len(),
                min: required,
            });
        }

        buf[0] = (self.version & DATA_TRACK_VERSION_MASK) << DATA_TRACK_VERSION_SHIFT;
        if self.is_start_of_frame {
            buf[0] |= 1 << DATA_TRACK_START_OF_FRAME_SHIFT;
        }
        if self.is_final_of_frame {
            buf[0] |= 1 << DATA_TRACK_FINAL_OF_FRAME_SHIFT;
        }

        let has_extensions = self.has_extensions || !self.extensions.is_empty();
        if has_extensions {
            buf[0] |= 1 << DATA_TRACK_EXTENSIONS_SHIFT;
        }

        buf[2..4].copy_from_slice(&self.handle.to_be_bytes());
        buf[4..6].copy_from_slice(&self.sequence_number.to_be_bytes());
        buf[6..8].copy_from_slice(&self.frame_number.to_be_bytes());
        buf[8..12].copy_from_slice(&self.timestamp.to_be_bytes());

        if !has_extensions {
            return Ok(DATA_TRACK_HEADER_LENGTH);
        }

        let extension_payload_size = self
            .extensions
            .iter()
            .map(|extension| {
                DATA_TRACK_EXTENSION_ID_LENGTH
                    + DATA_TRACK_EXTENSION_SIZE_LENGTH
                    + extension.data.len()
            })
            .sum::<usize>();
        let extension_block_size = DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH + extension_payload_size;
        let padded_extension_block_size = extension_block_size.div_ceil(4) * 4;

        let extensions_size_words_minus_one = (padded_extension_block_size / 4).saturating_sub(1);
        buf[12..14].copy_from_slice(&(extensions_size_words_minus_one as u16).to_be_bytes());

        let mut idx = DATA_TRACK_HEADER_LENGTH + DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH;
        for extension in &self.extensions {
            if extension.data.len() > u8::MAX as usize {
                return Err(DataTrackPacketError::ExtensionSizeTooBig {
                    size: extension.data.len(),
                });
            }
            buf[idx] = extension.id;
            buf[idx + 1] = extension.data.len() as u8;
            let data_start =
                idx + DATA_TRACK_EXTENSION_ID_LENGTH + DATA_TRACK_EXTENSION_SIZE_LENGTH;
            let data_end = data_start + extension.data.len();
            buf[data_start..data_end].copy_from_slice(&extension.data);
            idx = data_end;
        }

        let extension_data_and_padding_size =
            padded_extension_block_size - DATA_TRACK_EXTENSIONS_SIZE_FIELD_LENGTH;
        let extension_data_size = extension_payload_size;
        let padding_size = extension_data_and_padding_size.saturating_sub(extension_data_size);
        buf[idx..idx + padding_size].fill(0);

        Ok(DATA_TRACK_HEADER_LENGTH + padded_extension_block_size)
    }

    pub(crate) fn add_extension(&mut self, extension: DataTrackExtension) {
        if let Some(existing) = self
            .extensions
            .iter_mut()
            .find(|existing| existing.id == extension.id)
        {
            existing.data = extension.data;
            self.has_extensions = true;
            return;
        }

        self.extensions.push(extension);
        self.has_extensions = true;
    }

    pub(crate) fn get_extension(&self, id: u8) -> Result<DataTrackExtension, DataTrackPacketError> {
        self.extensions
            .iter()
            .find(|extension| extension.id == id)
            .cloned()
            .ok_or(DataTrackPacketError::ExtensionNotFound { id })
    }
}

impl DataTrackPacket {
    pub(crate) fn unmarshal(buf: &[u8]) -> Result<Self, DataTrackPacketError> {
        let (header, header_size) = DataTrackHeader::unmarshal(buf)?;
        Ok(Self {
            header,
            payload: buf[header_size..].to_vec(),
        })
    }

    pub(crate) fn marshal(&self) -> Result<Vec<u8>, DataTrackPacketError> {
        let mut buf = vec![0_u8; self.header.marshal_size() + self.payload.len()];
        self.marshal_to(&mut buf)?;
        Ok(buf)
    }

    pub(crate) fn marshal_to(&self, buf: &mut [u8]) -> Result<(), DataTrackPacketError> {
        let required = self.header.marshal_size() + self.payload.len();
        if buf.len() < required {
            return Err(DataTrackPacketError::BufferSizeInsufficient {
                actual: buf.len(),
                min: required,
            });
        }

        let header_size = self.header.marshal_to(buf)?;
        buf[header_size..header_size + self.payload.len()].copy_from_slice(&self.payload);
        Ok(())
    }
}

impl DataTrackExtensionParticipantSid {
    pub(crate) fn new(participant_sid: &str) -> Result<Self, DataTrackPacketError> {
        if participant_sid.len() >= 256 {
            return Err(DataTrackPacketError::ParticipantSidTooLong {
                len: participant_sid.len(),
            });
        }
        Ok(Self {
            participant_sid: participant_sid.to_string(),
        })
    }

    pub(crate) fn participant_sid(&self) -> &str {
        &self.participant_sid
    }

    pub(crate) fn marshal(&self) -> DataTrackExtension {
        DataTrackExtension {
            id: DATA_TRACK_EXTENSION_ID_PARTICIPANT_SID,
            data: self.participant_sid.as_bytes().to_vec(),
        }
    }

    pub(crate) fn unmarshal(extension: &DataTrackExtension) -> Result<Self, DataTrackPacketError> {
        if extension.id != DATA_TRACK_EXTENSION_ID_PARTICIPANT_SID {
            return Err(DataTrackPacketError::InvalidExtensionId {
                expected: DATA_TRACK_EXTENSION_ID_PARTICIPANT_SID,
                actual: extension.id,
            });
        }
        if extension.data.is_empty() {
            return Err(DataTrackPacketError::EmptyExtensionData);
        }
        let participant_sid = std::str::from_utf8(&extension.data)
            .map_err(|_| DataTrackPacketError::InvalidUtf8ParticipantSid)?
            .to_string();
        Ok(Self { participant_sid })
    }
}

pub(crate) fn data_track_packet_handle(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < DATA_TRACK_HEADER_LENGTH {
        return None;
    }
    Some(u16::from_be_bytes([bytes[2], bytes[3]]) as u32)
}

pub(crate) fn rewrite_data_track_packet_handle(bytes: &[u8], handle: u32) -> Option<Vec<u8>> {
    let handle = u16::try_from(handle).ok()?;
    let mut packet = bytes.to_vec();
    if packet.len() < DATA_TRACK_HEADER_LENGTH {
        return None;
    }
    packet[2..4].copy_from_slice(&handle.to_be_bytes());
    Some(packet)
}

#[cfg(test)]
mod tests {
    use super::{
        DATA_TRACK_EXTENSION_ID_PARTICIPANT_SID, DataTrackExtensionParticipantSid, DataTrackHeader,
        DataTrackPacket, DataTrackPacketError,
    };

    #[test]
    fn extension_participant_sid_rejects_too_long_and_roundtrips() {
        let too_long = "a".repeat(256);
        let error = DataTrackExtensionParticipantSid::new(&too_long)
            .expect_err("must reject sid >= 256 bytes");
        assert!(matches!(
            error,
            DataTrackPacketError::ParticipantSidTooLong { len: 256 }
        ));

        let extension = DataTrackExtensionParticipantSid::new("test")
            .expect("participant sid extension should build")
            .marshal();
        assert_eq!(extension.id, DATA_TRACK_EXTENSION_ID_PARTICIPANT_SID);
        assert_eq!(extension.data, b"test".to_vec());

        let unmarshaled = DataTrackExtensionParticipantSid::unmarshal(&extension)
            .expect("participant sid extension should unmarshal");
        assert_eq!(unmarshaled.participant_sid(), "test");
    }

    #[test]
    fn packet_marshal_unmarshal_without_extension_matches_upstream_wire_shape() {
        let packet = DataTrackPacket {
            header: DataTrackHeader {
                version: 0,
                is_start_of_frame: true,
                is_final_of_frame: true,
                has_extensions: false,
                handle: 3333,
                sequence_number: 6666,
                frame_number: 9999,
                timestamp: 0xdeadbeef,
                extensions: Vec::new(),
            },
            payload: vec![0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa],
        };

        let encoded = packet.marshal().expect("packet should marshal");
        let expected = vec![
            0x18, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0xff, 0xfe,
            0xfd, 0xfc, 0xfb, 0xfa,
        ];
        assert_eq!(encoded, expected);

        let decoded = DataTrackPacket::unmarshal(&encoded).expect("packet should unmarshal");
        assert_eq!(decoded, packet);
    }

    #[test]
    fn packet_marshal_unmarshal_with_extension_padding_and_replace() {
        let mut header = DataTrackHeader {
            version: 0,
            is_start_of_frame: true,
            is_final_of_frame: false,
            has_extensions: false,
            handle: 3333,
            sequence_number: 6666,
            frame_number: 9999,
            timestamp: 0xdeadbeef,
            extensions: Vec::new(),
        };
        header.add_extension(
            DataTrackExtensionParticipantSid::new("participant")
                .expect("sid extension should build")
                .marshal(),
        );

        let mut packet = DataTrackPacket {
            header,
            payload: vec![0xff, 0xfe, 0xfd, 0xfc],
        };

        let encoded = packet.marshal().expect("packet should marshal");
        let expected = vec![
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x03,
            0x01, 0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00,
            0xff, 0xfe, 0xfd, 0xfc,
        ];
        assert_eq!(encoded, expected);

        packet.header.add_extension(
            DataTrackExtensionParticipantSid::new("test_participant")
                .expect("sid extension should build")
                .marshal(),
        );
        let replaced = packet
            .marshal()
            .expect("packet should marshal after replacement");
        let expected_replaced = vec![
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x04,
            0x01, 0x10, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69,
            0x70, 0x61, 0x6e, 0x74, 0xff, 0xfe, 0xfd, 0xfc,
        ];
        assert_eq!(replaced, expected_replaced);

        let decoded = DataTrackPacket::unmarshal(&replaced).expect("packet should unmarshal");
        let extension = decoded
            .header
            .get_extension(DATA_TRACK_EXTENSION_ID_PARTICIPANT_SID)
            .expect("extension should exist");
        let sid = DataTrackExtensionParticipantSid::unmarshal(&extension)
            .expect("sid extension should unmarshal");
        assert_eq!(sid.participant_sid(), "test_participant");
    }

    #[test]
    fn packet_unmarshal_rejects_invalid_extension_sizes() {
        let bad_packet_small = vec![
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x02,
            0x01, 0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00,
            0xff, 0xfe, 0xfd, 0xfc,
        ];
        assert!(DataTrackPacket::unmarshal(&bad_packet_small).is_err());

        let bad_packet_big = vec![
            0x14, 0x00, 0x0d, 0x05, 0x1a, 0x0a, 0x27, 0x0f, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x03,
            0x01, 0x0d, 0x70, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x00,
            0xff, 0xfe, 0xfd, 0xfc,
        ];
        assert!(DataTrackPacket::unmarshal(&bad_packet_big).is_err());
    }
}
