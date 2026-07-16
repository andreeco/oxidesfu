use std::collections::{HashMap, VecDeque};

const MISSING_PICTURE_IDS_THRESHOLD: usize = 50;
const DROPPED_PICTURE_IDS_THRESHOLD: usize = 20;
const EXEMPTED_PICTURE_IDS_THRESHOLD: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Vp8MungerError {
    NotVp8,
    OutOfOrderVp8PictureIdCacheMiss,
    FilteredVp8TemporalLayer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Vp8Header {
    pub(crate) first_byte: u8,
    pub(crate) i: bool,
    pub(crate) m: bool,
    pub(crate) picture_id: u16,
    pub(crate) l: bool,
    pub(crate) tl0_pic_idx: u8,
    pub(crate) t: bool,
    pub(crate) tid: u8,
    pub(crate) y: bool,
    pub(crate) k: bool,
    pub(crate) key_idx: u8,
    pub(crate) header_size: usize,
    pub(crate) is_key_frame: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ExtPacketVp8 {
    pub(crate) sequence_number: u16,
    pub(crate) timestamp: u32,
    pub(crate) temporal: i32,
    pub(crate) payload: Option<Vp8Header>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Vp8PictureIdWrapHandler {
    max_picture_id: i32,
    max_m_bit: bool,
    total_wrap: i32,
    last_wrap: i32,
}

impl Vp8PictureIdWrapHandler {
    pub(crate) fn new() -> Self {
        Self {
            max_picture_id: -1,
            max_m_bit: false,
            total_wrap: 0,
            last_wrap: 0,
        }
    }

    pub(crate) fn init(&mut self, ext_picture_id: i32, m_bit: bool) {
        self.max_picture_id = ext_picture_id;
        self.max_m_bit = m_bit;
        self.total_wrap = 0;
        self.last_wrap = 0;
    }

    pub(crate) fn max_picture_id(&self) -> i32 {
        self.max_picture_id
    }

    pub(crate) fn update_max_picture_id(&mut self, ext_picture_id: i32, m_bit: bool) {
        self.max_picture_id = ext_picture_id;
        self.max_m_bit = m_bit;
    }

    pub(crate) fn unwrap(&self, picture_id: u16, m_bit: bool) -> i32 {
        let mut max_picture_id = self.max_picture_id;
        if max_picture_id > 0 {
            if self.max_m_bit {
                max_picture_id &= 0x7fff;
            } else {
                max_picture_id &= 0x7f;
            }
        }

        let mut new_picture_id = if m_bit {
            i32::from(picture_id & 0x7fff)
        } else {
            i32::from(picture_id & 0x7f)
        };

        if self.total_wrap > 0
            && (self.max_picture_id + (self.last_wrap >> 1)) < (new_picture_id + self.total_wrap)
        {
            return new_picture_id + self.total_wrap - self.last_wrap;
        }

        let mut wrap = 0;
        if self.max_m_bit {
            if is_wrapping_15_bit(max_picture_id, new_picture_id) {
                wrap = 1 << 15;
            }
        } else if is_wrapping_7_bit(max_picture_id, new_picture_id) {
            wrap = 1 << 7;
        }

        new_picture_id += self.total_wrap + wrap;
        new_picture_id
    }

    pub(crate) fn unwrap_and_update(&mut self, picture_id: u16, m_bit: bool) -> i32 {
        let mut max_picture_id = self.max_picture_id;
        if max_picture_id > 0 {
            if self.max_m_bit {
                max_picture_id &= 0x7fff;
            } else {
                max_picture_id &= 0x7f;
            }
        }

        let mut new_picture_id = if m_bit {
            i32::from(picture_id & 0x7fff)
        } else {
            i32::from(picture_id & 0x7f)
        };

        if self.total_wrap > 0
            && (self.max_picture_id + (self.last_wrap >> 1)) < (new_picture_id + self.total_wrap)
        {
            return new_picture_id + self.total_wrap - self.last_wrap;
        }

        let mut wrap = 0;
        if self.max_m_bit {
            if is_wrapping_15_bit(max_picture_id, new_picture_id) {
                wrap = 1 << 15;
            }
        } else if is_wrapping_7_bit(max_picture_id, new_picture_id) {
            wrap = 1 << 7;
        }

        self.total_wrap += wrap;
        if wrap != 0 {
            self.last_wrap = wrap;
        }

        new_picture_id += self.total_wrap;
        new_picture_id
    }
}

fn is_wrapping_7_bit(val1: i32, val2: i32) -> bool {
    val2 < val1 && (val1 - val2) > (1 << 6)
}

fn is_wrapping_15_bit(val1: i32, val2: i32) -> bool {
    val2 < val1 && (val1 - val2) > (1 << 14)
}

#[derive(Debug, Clone)]
struct OrderedCache<K: Eq + std::hash::Hash + Clone, V> {
    map: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K: Eq + std::hash::Hash + Clone, V> OrderedCache<K, V> {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }

    fn set(&mut self, key: K, value: V) {
        if !self.map.contains_key(&key) {
            self.order.push_back(key.clone());
        }
        self.map.insert(key, value);
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn pop_front(&mut self) {
        if let Some(key) = self.order.pop_front() {
            self.map.remove(&key);
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct Vp8Munger {
    picture_id_wrap_handler: Vp8PictureIdWrapHandler,
    ext_last_picture_id: i32,
    picture_id_offset: i32,
    picture_id_used: bool,
    last_tl0_pic_idx: u8,
    tl0_pic_idx_offset: u8,
    tl0_pic_idx_used: bool,
    tid_used: bool,
    last_key_idx: u8,
    key_idx_offset: u8,
    key_idx_used: bool,
    missing_picture_ids: OrderedCache<i32, i32>,
    dropped_picture_ids: OrderedCache<i32, bool>,
    exempted_picture_ids: OrderedCache<i32, bool>,
}

#[allow(dead_code)]
impl Vp8Munger {
    pub(crate) fn new() -> Self {
        Self {
            picture_id_wrap_handler: Vp8PictureIdWrapHandler::new(),
            ext_last_picture_id: 0,
            picture_id_offset: 0,
            picture_id_used: false,
            last_tl0_pic_idx: 0,
            tl0_pic_idx_offset: 0,
            tl0_pic_idx_used: false,
            tid_used: false,
            last_key_idx: 0,
            key_idx_offset: 0,
            key_idx_used: false,
            missing_picture_ids: OrderedCache::new(),
            dropped_picture_ids: OrderedCache::new(),
            exempted_picture_ids: OrderedCache::new(),
        }
    }

    pub(crate) fn set_last(&mut self, ext_pkt: &ExtPacketVp8) {
        let Some(vp8) = &ext_pkt.payload else {
            return;
        };

        self.picture_id_used = vp8.i;
        if self.picture_id_used {
            self.picture_id_wrap_handler
                .init(i32::from(vp8.picture_id) - 1, vp8.m);
            self.ext_last_picture_id = i32::from(vp8.picture_id);
        }

        self.tl0_pic_idx_used = vp8.l;
        if self.tl0_pic_idx_used {
            self.last_tl0_pic_idx = vp8.tl0_pic_idx;
        }

        self.tid_used = vp8.t;

        self.key_idx_used = vp8.k;
        if self.key_idx_used {
            self.last_key_idx = vp8.key_idx;
        }
    }

    pub(crate) fn update_offsets(&mut self, ext_pkt: &ExtPacketVp8) {
        let Some(vp8) = &ext_pkt.payload else {
            return;
        };

        if self.picture_id_used {
            self.picture_id_wrap_handler
                .init(i32::from(vp8.picture_id) - 1, vp8.m);
            self.picture_id_offset = i32::from(vp8.picture_id) - self.ext_last_picture_id - 1;
        }

        if self.tl0_pic_idx_used {
            self.tl0_pic_idx_offset = vp8
                .tl0_pic_idx
                .wrapping_sub(self.last_tl0_pic_idx)
                .wrapping_sub(1);
        }

        if self.key_idx_used {
            self.key_idx_offset =
                vp8.key_idx.wrapping_sub(self.last_key_idx).wrapping_sub(1) & 0x1f;
        }

        self.missing_picture_ids.clear();
        self.dropped_picture_ids.clear();
        self.exempted_picture_ids.clear();
    }

    pub(crate) fn update_and_get(
        &mut self,
        ext_pkt: &ExtPacketVp8,
        sn_out_of_order: bool,
        sn_has_gap: bool,
        max_temporal_layer: i32,
    ) -> Result<(usize, Vp8Header), Vp8MungerError> {
        let vp8 = ext_pkt.payload.as_ref().ok_or(Vp8MungerError::NotVp8)?;

        let ext_picture_id = self
            .picture_id_wrap_handler
            .unwrap_and_update(vp8.picture_id, vp8.m);

        if sn_out_of_order {
            let picture_id_offset = self
                .missing_picture_ids
                .get(&ext_picture_id)
                .copied()
                .ok_or(Vp8MungerError::OutOfOrderVp8PictureIdCacheMiss)?;

            let munged_picture_id = ((ext_picture_id - picture_id_offset) & 0x7fff) as u16;
            let out = Vp8Header {
                first_byte: vp8.first_byte,
                i: vp8.i,
                m: munged_picture_id > 127,
                picture_id: munged_picture_id,
                l: vp8.l,
                tl0_pic_idx: vp8.tl0_pic_idx.wrapping_sub(self.tl0_pic_idx_offset),
                t: vp8.t,
                tid: vp8.tid,
                y: vp8.y,
                k: vp8.k,
                key_idx: vp8.key_idx.wrapping_sub(self.key_idx_offset),
                is_key_frame: vp8.is_key_frame,
                header_size: adjusted_header_size(vp8.header_size, munged_picture_id > 127, vp8.m),
            };
            return Ok((vp8.header_size, out));
        }

        let prev_max_picture_id = self.picture_id_wrap_handler.max_picture_id();
        self.picture_id_wrap_handler
            .update_max_picture_id(ext_picture_id, vp8.m);

        if sn_has_gap {
            for lost_picture_id in prev_max_picture_id..=ext_picture_id {
                if self.dropped_picture_ids.get(&lost_picture_id).is_none() {
                    self.missing_picture_ids
                        .set(lost_picture_id, self.picture_id_offset);
                }
            }
            while self.missing_picture_ids.len() > MISSING_PICTURE_IDS_THRESHOLD {
                self.missing_picture_ids.pop_front();
            }

            if ext_pkt.temporal > max_temporal_layer {
                self.exempted_picture_ids.set(ext_picture_id, true);
                while self.exempted_picture_ids.len() > EXEMPTED_PICTURE_IDS_THRESHOLD {
                    self.exempted_picture_ids.pop_front();
                }
            }
        } else if ext_pkt.temporal > max_temporal_layer
            && self.exempted_picture_ids.get(&ext_picture_id).is_none()
        {
            if vp8.i && prev_max_picture_id != ext_picture_id {
                self.dropped_picture_ids.set(ext_picture_id, true);
                while self.dropped_picture_ids.len() > DROPPED_PICTURE_IDS_THRESHOLD {
                    self.dropped_picture_ids.pop_front();
                }
                self.picture_id_offset += 1;
            }
            return Err(Vp8MungerError::FilteredVp8TemporalLayer);
        }

        let ext_munged_picture_id = ext_picture_id - self.picture_id_offset;
        let munged_picture_id = (ext_munged_picture_id & 0x7fff) as u16;
        let munged_tl0 = vp8.tl0_pic_idx.wrapping_sub(self.tl0_pic_idx_offset);
        let munged_key_idx = vp8.key_idx.wrapping_sub(self.key_idx_offset) & 0x1f;

        self.ext_last_picture_id = ext_munged_picture_id;
        self.last_tl0_pic_idx = munged_tl0;
        self.last_key_idx = munged_key_idx;

        let out = Vp8Header {
            first_byte: vp8.first_byte,
            i: vp8.i,
            m: munged_picture_id > 127,
            picture_id: munged_picture_id,
            l: vp8.l,
            tl0_pic_idx: munged_tl0,
            t: vp8.t,
            tid: vp8.tid,
            y: vp8.y,
            k: vp8.k,
            key_idx: munged_key_idx,
            is_key_frame: vp8.is_key_frame,
            header_size: adjusted_header_size(vp8.header_size, munged_picture_id > 127, vp8.m),
        };

        Ok((vp8.header_size, out))
    }

    pub(crate) fn update_and_get_padding(
        &mut self,
        new_picture: bool,
    ) -> Result<Vp8Header, Vp8MungerError> {
        let offset = if new_picture { 1 } else { 0 };

        let mut header_size = 1usize;
        if self.picture_id_used || self.tl0_pic_idx_used || self.tid_used || self.key_idx_used {
            header_size += 1;
        }

        let mut ext_picture_id = self.ext_last_picture_id;
        if self.picture_id_used {
            ext_picture_id += offset;
            self.ext_last_picture_id = ext_picture_id;
            self.picture_id_offset -= offset;
            if (ext_picture_id & 0x7fff) > 127 {
                header_size += 2;
            } else {
                header_size += 1;
            }
        }
        let picture_id = (ext_picture_id & 0x7fff) as u16;

        let mut tl0 = 0u8;
        if self.tl0_pic_idx_used {
            tl0 = self.last_tl0_pic_idx.wrapping_add(offset as u8);
            self.last_tl0_pic_idx = tl0;
            self.tl0_pic_idx_offset = self.tl0_pic_idx_offset.wrapping_sub(offset as u8);
            header_size += 1;
        }

        if self.tid_used || self.key_idx_used {
            header_size += 1;
        }

        let mut key_idx = 0u8;
        if self.key_idx_used {
            key_idx = self.last_key_idx.wrapping_add(offset as u8) & 0x1f;
            self.last_key_idx = key_idx;
            self.key_idx_offset = self.key_idx_offset.wrapping_sub(offset as u8);
        }

        Ok(Vp8Header {
            first_byte: 0x10,
            i: self.picture_id_used,
            m: picture_id > 127,
            picture_id,
            l: self.tl0_pic_idx_used,
            tl0_pic_idx: tl0,
            t: self.tid_used,
            tid: 0,
            y: true,
            k: self.key_idx_used,
            key_idx,
            is_key_frame: true,
            header_size,
        })
    }

    pub(crate) fn picture_id_offset(&self, ext_picture_id: i32) -> Option<i32> {
        self.missing_picture_ids.get(&ext_picture_id).copied()
    }

    #[cfg(test)]
    fn state_view(&self) -> (i32, i32, u8, u8, u8, u8, bool, bool, bool, bool) {
        (
            self.picture_id_wrap_handler.max_picture_id,
            self.ext_last_picture_id,
            self.last_tl0_pic_idx,
            self.tl0_pic_idx_offset,
            self.last_key_idx,
            self.key_idx_offset,
            self.picture_id_used,
            self.tl0_pic_idx_used,
            self.tid_used,
            self.key_idx_used,
        )
    }
}

fn adjusted_header_size(base: usize, new_m_bit: bool, old_m_bit: bool) -> usize {
    match (new_m_bit, old_m_bit) {
        (true, false) => base + 1,
        (false, true) => base.saturating_sub(1),
        _ => base,
    }
}

#[cfg(test)]
mod tests {
    use super::{ExtPacketVp8, Vp8Header, Vp8Munger, Vp8MungerError, Vp8PictureIdWrapHandler};

    fn packet(sequence_number: u16, timestamp: u32, temporal: i32, vp8: Vp8Header) -> ExtPacketVp8 {
        ExtPacketVp8 {
            sequence_number,
            timestamp,
            temporal,
            payload: Some(vp8),
        }
    }

    fn base_vp8(picture_id: u16, tid: u8) -> Vp8Header {
        Vp8Header {
            first_byte: 25,
            i: true,
            m: true,
            picture_id,
            l: true,
            tl0_pic_idx: 233,
            t: true,
            tid,
            y: true,
            k: true,
            key_idx: 23,
            header_size: 6,
            is_key_frame: true,
        }
    }

    #[test]
    fn vp8_munger_set_last_matches_upstream_contract() {
        let mut v = Vp8Munger::new();
        let ext_pkt = packet(23333, 0xabcdef, 1, base_vp8(13467, 1));

        v.set_last(&ext_pkt);

        let state = v.state_view();
        assert_eq!(state.0, 13466);
        assert_eq!(state.1, 13467);
        assert_eq!(state.2, 233);
        assert!(state.6);
        assert!(state.7);
        assert!(state.8);
        assert!(state.9);
    }

    #[test]
    fn vp8_munger_update_offsets_matches_upstream_contract() {
        let mut v = Vp8Munger::new();
        let first = packet(23333, 0xabcdef, 1, base_vp8(13467, 1));
        v.set_last(&first);

        let switched = packet(
            56789,
            0xabcdef,
            1,
            Vp8Header {
                picture_id: 345,
                tl0_pic_idx: 12,
                key_idx: 4,
                ..base_vp8(345, 1)
            },
        );

        v.update_offsets(&switched);
        let state = v.state_view();
        assert_eq!(state.0, 344);
        assert_eq!(state.1, 13467);
        assert_eq!(v.picture_id_offset, 345 - 13467 - 1);
        assert_eq!(state.3, 12u8.wrapping_sub(233).wrapping_sub(1));
        assert_eq!(state.5, (4u8.wrapping_sub(23).wrapping_sub(1)) & 0x1f);
    }

    #[test]
    fn vp8_munger_out_of_order_picture_id_matches_upstream_contract() {
        let mut v = Vp8Munger::new();
        let ext_pkt = packet(23333, 0xabcdef, 1, base_vp8(13467, 1));
        v.set_last(&ext_pkt);
        v.update_and_get(&ext_pkt, false, false, 2)
            .expect("initial translation should succeed");

        let old = packet(23333, 0xabcdef, 1, base_vp8(13466, 1));
        let err = v
            .update_and_get(&old, true, false, 2)
            .expect_err("out-of-order without cache entry should fail");
        assert_eq!(err, Vp8MungerError::OutOfOrderVp8PictureIdCacheMiss);

        let gapped = packet(23333, 0xabcdef, 1, base_vp8(13469, 1));
        let (n_in, out) = v
            .update_and_get(&gapped, false, true, 2)
            .expect("gapped in-order packet should forward");
        assert_eq!(n_in, 6);
        assert_eq!(out.picture_id, 13469);

        assert_eq!(v.picture_id_offset(13467), Some(0));
        assert_eq!(v.picture_id_offset(13468), Some(0));
        assert_eq!(v.picture_id_offset(13469), Some(0));

        let missing_now_present = packet(23333, 0xabcdef, 1, base_vp8(13468, 1));
        let (n_in, out) = v
            .update_and_get(&missing_now_present, true, false, 2)
            .expect("cached out-of-order picture should translate");
        assert_eq!(n_in, 6);
        assert_eq!(out.picture_id, 13468);
    }

    #[test]
    fn vp8_munger_temporal_layer_filtering_matches_upstream_contract() {
        let mut v = Vp8Munger::new();
        let mut ext_pkt = packet(23333, 0xabcdef, 1, base_vp8(13467, 1));
        v.set_last(&ext_pkt);

        let err = v
            .update_and_get(&ext_pkt, false, false, 0)
            .expect_err("packet above max temporal layer should be filtered");
        assert_eq!(err, Vp8MungerError::FilteredVp8TemporalLayer);
        assert_eq!(v.picture_id_offset, 1);

        ext_pkt.sequence_number = 23334;
        let err = v
            .update_and_get(&ext_pkt, false, false, 0)
            .expect_err("same picture repeat should still be filtered");
        assert_eq!(err, Vp8MungerError::FilteredVp8TemporalLayer);
        assert_eq!(v.picture_id_offset, 1);

        ext_pkt.sequence_number = 23337;
        let err = v
            .update_and_get(&ext_pkt, false, false, 0)
            .expect_err("gap with same picture should still not double-count offset");
        assert_eq!(err, Vp8MungerError::FilteredVp8TemporalLayer);
        assert_eq!(v.picture_id_offset, 1);
    }

    #[test]
    fn vp8_munger_gap_in_sequence_number_same_picture_matches_upstream_contract() {
        let mut v = Vp8Munger::new();
        let ext_pkt = packet(65533, 0xabcdef, 1, base_vp8(13467, 1));
        v.set_last(&ext_pkt);

        let (_n_in, out) = v
            .update_and_get(&ext_pkt, false, false, 2)
            .expect("first translate should succeed");
        assert_eq!(out.picture_id, 13467);

        let (_n_in, out) = v
            .update_and_get(&ext_pkt, false, true, 2)
            .expect("gapped translate should succeed");
        assert_eq!(out.picture_id, 13467);

        assert_eq!(v.picture_id_offset(13467), Some(0));
    }

    #[test]
    fn vp8_munger_update_and_get_padding_matches_upstream_contract() {
        let mut v = Vp8Munger::new();
        let ext_pkt = packet(23333, 0xabcdef, 1, base_vp8(13467, 13));
        v.set_last(&ext_pkt);

        let repeated = v
            .update_and_get_padding(false)
            .expect("repeat padding should succeed");
        assert_eq!(repeated.first_byte, 0x10);
        assert_eq!(repeated.picture_id, 13467);
        assert_eq!(repeated.tl0_pic_idx, 233);
        assert_eq!(repeated.tid, 0);
        assert_eq!(repeated.key_idx, 23);

        let new_picture = v
            .update_and_get_padding(true)
            .expect("new picture padding should succeed");
        assert_eq!(new_picture.picture_id, 13468);
        assert_eq!(new_picture.tl0_pic_idx, 234);
        assert_eq!(new_picture.key_idx, 24);
    }

    #[test]
    fn vp8_picture_id_wrap_handler_matches_upstream_contract() {
        let mut v = Vp8PictureIdWrapHandler::new();

        v.init(109, false);
        assert_eq!(v.max_picture_id(), 109);
        assert!(!v.max_m_bit);

        v.update_max_picture_id(109350, true);
        assert_eq!(v.max_picture_id(), 109350);
        assert!(v.max_m_bit);

        v.init(32766, true);
        let ext_picture_id = v.unwrap_and_update(32750, true);
        assert_eq!(ext_picture_id, 32750);
        assert_eq!(v.total_wrap, 0);
        assert_eq!(v.last_wrap, 0);

        let ext_picture_id = v.unwrap_and_update(5, false);
        assert_eq!(ext_picture_id, 32773);
        assert_eq!(v.total_wrap, 32768);
        assert_eq!(v.last_wrap, 32768);

        v.update_max_picture_id(32893, false);
        let ext_picture_id = v.unwrap_and_update(5, true);
        assert_eq!(ext_picture_id, 32901);
        assert_eq!(v.total_wrap, 32896);
        assert_eq!(v.last_wrap, 128);

        v.update_max_picture_id(32901, false);
        let ext_picture_id = v.unwrap_and_update(73, false);
        assert_eq!(ext_picture_id, 32841);

        v.update_max_picture_id(32901, true);
        v.last_wrap = 32768;
        let ext_picture_id = v.unwrap_and_update(73, false);
        assert_eq!(ext_picture_id, 32969);
    }
}
