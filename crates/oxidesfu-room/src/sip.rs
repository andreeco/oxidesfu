use std::collections::HashSet;

use livekit_protocol as proto;

use crate::{RoomStore, RoomStoreError};

fn sip_trunk_to_inbound(info: &proto::SipTrunkInfo) -> proto::SipInboundTrunkInfo {
    let numbers = if info.inbound_numbers.is_empty() && !info.outbound_number.is_empty() {
        vec![info.outbound_number.clone()]
    } else {
        info.inbound_numbers.clone()
    };

    proto::SipInboundTrunkInfo {
        sip_trunk_id: info.sip_trunk_id.clone(),
        name: info.name.clone(),
        metadata: info.metadata.clone(),
        numbers,
        allowed_addresses: info.inbound_addresses.clone(),
        auth_username: info.inbound_username.clone(),
        auth_password: info.inbound_password.clone(),
        ..Default::default()
    }
}

fn sip_trunk_to_outbound(info: &proto::SipTrunkInfo) -> proto::SipOutboundTrunkInfo {
    let mut numbers = Vec::new();
    if !info.outbound_number.is_empty() {
        numbers.push(info.outbound_number.clone());
    }

    proto::SipOutboundTrunkInfo {
        sip_trunk_id: info.sip_trunk_id.clone(),
        name: info.name.clone(),
        metadata: info.metadata.clone(),
        address: info.outbound_address.clone(),
        numbers,
        auth_username: info.outbound_username.clone(),
        auth_password: info.outbound_password.clone(),
        transport: info.transport,
        ..Default::default()
    }
}

fn sip_inbound_to_legacy(info: &proto::SipInboundTrunkInfo) -> proto::SipTrunkInfo {
    proto::SipTrunkInfo {
        sip_trunk_id: info.sip_trunk_id.clone(),
        kind: proto::sip_trunk_info::TrunkKind::TrunkInbound as i32,
        inbound_numbers: info.numbers.clone(),
        inbound_addresses: info.allowed_addresses.clone(),
        inbound_username: info.auth_username.clone(),
        inbound_password: info.auth_password.clone(),
        name: info.name.clone(),
        metadata: info.metadata.clone(),
        ..Default::default()
    }
}

fn sip_outbound_to_legacy(info: &proto::SipOutboundTrunkInfo) -> proto::SipTrunkInfo {
    proto::SipTrunkInfo {
        sip_trunk_id: info.sip_trunk_id.clone(),
        kind: proto::sip_trunk_info::TrunkKind::TrunkOutbound as i32,
        outbound_address: info.address.clone(),
        outbound_number: info.numbers.first().cloned().unwrap_or_default(),
        outbound_username: info.auth_username.clone(),
        outbound_password: info.auth_password.clone(),
        transport: info.transport,
        name: info.name.clone(),
        metadata: info.metadata.clone(),
        ..Default::default()
    }
}

fn collect_after_limit<T>(
    mut items: Vec<T>,
    page: Option<&proto::Pagination>,
    id_of: impl Fn(&T) -> &str,
) -> Vec<T> {
    items.sort_by(|a, b| id_of(a).cmp(id_of(b)));

    if let Some(page) = page {
        if !page.after_id.is_empty() {
            items.retain(|item| id_of(item) > page.after_id.as_str());
        }
        if page.limit > 0 {
            let limit = page.limit as usize;
            if items.len() > limit {
                items.truncate(limit);
            }
        }
    }

    items
}

fn matches_numbers_filter(filter_numbers: &[String], trunk_numbers: &[String]) -> bool {
    if filter_numbers.is_empty() {
        return true;
    }

    if trunk_numbers.is_empty() {
        return true;
    }

    let filter: HashSet<&str> = filter_numbers.iter().map(String::as_str).collect();
    trunk_numbers
        .iter()
        .map(String::as_str)
        .any(|number| filter.contains(number))
}

fn matches_ids_filter(filter_ids: &[String], id: &str) -> bool {
    filter_ids.is_empty() || filter_ids.iter().any(|candidate| candidate == id)
}

impl RoomStore {
    /// Stores a legacy SIP trunk.
    pub fn store_sip_trunk(&self, info: &proto::SipTrunkInfo) -> Result<(), RoomStoreError> {
        if info.sip_trunk_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "sip trunk id must not be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .sip_legacy_trunks
            .insert(info.sip_trunk_id.clone(), info.clone());
        Ok(())
    }

    /// Stores an inbound SIP trunk.
    pub fn store_sip_inbound_trunk(
        &self,
        info: &proto::SipInboundTrunkInfo,
    ) -> Result<(), RoomStoreError> {
        if info.sip_trunk_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "sip trunk id must not be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .sip_inbound_trunks
            .insert(info.sip_trunk_id.clone(), info.clone());
        Ok(())
    }

    /// Stores an outbound SIP trunk.
    pub fn store_sip_outbound_trunk(
        &self,
        info: &proto::SipOutboundTrunkInfo,
    ) -> Result<(), RoomStoreError> {
        if info.sip_trunk_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "sip trunk id must not be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .sip_outbound_trunks
            .insert(info.sip_trunk_id.clone(), info.clone());
        Ok(())
    }

    /// Loads a SIP trunk, including compatibility fallbacks from inbound/outbound forms.
    pub fn load_sip_trunk(
        &self,
        sip_trunk_id: &str,
    ) -> Result<proto::SipTrunkInfo, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        if let Some(info) = inner.sip_legacy_trunks.get(sip_trunk_id) {
            return Ok(info.clone());
        }
        if let Some(info) = inner.sip_inbound_trunks.get(sip_trunk_id) {
            return Ok(sip_inbound_to_legacy(info));
        }
        if let Some(info) = inner.sip_outbound_trunks.get(sip_trunk_id) {
            return Ok(sip_outbound_to_legacy(info));
        }

        Err(RoomStoreError::SipTrunkNotFound)
    }

    /// Loads an inbound SIP trunk, including compatibility fallback from a legacy trunk.
    pub fn load_sip_inbound_trunk(
        &self,
        sip_trunk_id: &str,
    ) -> Result<proto::SipInboundTrunkInfo, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        if let Some(info) = inner.sip_inbound_trunks.get(sip_trunk_id) {
            return Ok(info.clone());
        }
        if let Some(info) = inner.sip_legacy_trunks.get(sip_trunk_id) {
            return Ok(sip_trunk_to_inbound(info));
        }

        Err(RoomStoreError::SipTrunkNotFound)
    }

    /// Loads an outbound SIP trunk, including compatibility fallback from a legacy trunk.
    pub fn load_sip_outbound_trunk(
        &self,
        sip_trunk_id: &str,
    ) -> Result<proto::SipOutboundTrunkInfo, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        if let Some(info) = inner.sip_outbound_trunks.get(sip_trunk_id) {
            return Ok(info.clone());
        }
        if let Some(info) = inner.sip_legacy_trunks.get(sip_trunk_id) {
            return Ok(sip_trunk_to_outbound(info));
        }

        Err(RoomStoreError::SipTrunkNotFound)
    }

    /// Deletes all SIP trunk representations for an ID.
    pub fn delete_sip_trunk(&self, sip_trunk_id: &str) -> Result<(), RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner.sip_legacy_trunks.remove(sip_trunk_id);
        inner.sip_inbound_trunks.remove(sip_trunk_id);
        inner.sip_outbound_trunks.remove(sip_trunk_id);
        Ok(())
    }

    /// Lists SIP trunks (legacy + inbound + outbound compatibility forms).
    pub fn list_sip_trunk(
        &self,
        request: &proto::ListSipTrunkRequest,
    ) -> Result<proto::ListSipTrunkResponse, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        let mut items: Vec<proto::SipTrunkInfo> =
            inner.sip_legacy_trunks.values().cloned().collect();
        items.extend(inner.sip_inbound_trunks.values().map(sip_inbound_to_legacy));
        items.extend(
            inner
                .sip_outbound_trunks
                .values()
                .map(sip_outbound_to_legacy),
        );

        let items = collect_after_limit(items, request.page.as_ref(), |item| &item.sip_trunk_id);

        Ok(proto::ListSipTrunkResponse { items })
    }

    /// Lists inbound SIP trunks including compatibility entries converted from legacy trunks.
    pub fn list_sip_inbound_trunk(
        &self,
        request: &proto::ListSipInboundTrunkRequest,
    ) -> Result<proto::ListSipInboundTrunkResponse, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        let mut items: Vec<proto::SipInboundTrunkInfo> = Vec::new();

        for info in inner.sip_inbound_trunks.values() {
            if matches_ids_filter(&request.trunk_ids, &info.sip_trunk_id)
                && matches_numbers_filter(&request.numbers, &info.numbers)
            {
                items.push(info.clone());
            }
        }

        for info in inner.sip_legacy_trunks.values() {
            let converted = sip_trunk_to_inbound(info);
            if matches_ids_filter(&request.trunk_ids, &converted.sip_trunk_id)
                && matches_numbers_filter(&request.numbers, &converted.numbers)
            {
                items.push(converted);
            }
        }

        let items = collect_after_limit(items, request.page.as_ref(), |item| &item.sip_trunk_id);

        Ok(proto::ListSipInboundTrunkResponse { items })
    }

    /// Lists outbound SIP trunks including compatibility entries converted from legacy trunks.
    pub fn list_sip_outbound_trunk(
        &self,
        request: &proto::ListSipOutboundTrunkRequest,
    ) -> Result<proto::ListSipOutboundTrunkResponse, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        let mut items: Vec<proto::SipOutboundTrunkInfo> = Vec::new();

        for info in inner.sip_outbound_trunks.values() {
            if matches_ids_filter(&request.trunk_ids, &info.sip_trunk_id)
                && matches_numbers_filter(&request.numbers, &info.numbers)
            {
                items.push(info.clone());
            }
        }

        for info in inner.sip_legacy_trunks.values() {
            let converted = sip_trunk_to_outbound(info);
            if matches_ids_filter(&request.trunk_ids, &converted.sip_trunk_id)
                && matches_numbers_filter(&request.numbers, &converted.numbers)
            {
                items.push(converted);
            }
        }

        let items = collect_after_limit(items, request.page.as_ref(), |item| &item.sip_trunk_id);

        Ok(proto::ListSipOutboundTrunkResponse { items })
    }

    /// Stores a SIP dispatch rule.
    pub fn store_sip_dispatch_rule(
        &self,
        info: &proto::SipDispatchRuleInfo,
    ) -> Result<(), RoomStoreError> {
        if info.sip_dispatch_rule_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "sip dispatch rule id must not be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .sip_dispatch_rules
            .insert(info.sip_dispatch_rule_id.clone(), info.clone());
        Ok(())
    }

    /// Loads a SIP dispatch rule.
    pub fn load_sip_dispatch_rule(
        &self,
        sip_dispatch_rule_id: &str,
    ) -> Result<proto::SipDispatchRuleInfo, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .sip_dispatch_rules
            .get(sip_dispatch_rule_id)
            .cloned()
            .ok_or(RoomStoreError::SipDispatchRuleNotFound)
    }

    /// Deletes a SIP dispatch rule if it exists.
    pub fn delete_sip_dispatch_rule(
        &self,
        sip_dispatch_rule_id: &str,
    ) -> Result<(), RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner.sip_dispatch_rules.remove(sip_dispatch_rule_id);
        Ok(())
    }

    /// Lists SIP dispatch rules.
    pub fn list_sip_dispatch_rule(
        &self,
        request: &proto::ListSipDispatchRuleRequest,
    ) -> Result<proto::ListSipDispatchRuleResponse, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        let mut items: Vec<proto::SipDispatchRuleInfo> = Vec::new();
        for info in inner.sip_dispatch_rules.values() {
            if !matches_ids_filter(&request.dispatch_rule_ids, &info.sip_dispatch_rule_id) {
                continue;
            }

            if !request.trunk_ids.is_empty()
                && !info.trunk_ids.is_empty()
                && !info
                    .trunk_ids
                    .iter()
                    .any(|trunk_id| request.trunk_ids.iter().any(|req_id| req_id == trunk_id))
            {
                continue;
            }

            if !request.trunk_ids.is_empty() && info.trunk_ids.is_empty() {
                // wildcard rule always included when filtering by trunk id
            }

            items.push(info.clone());
        }

        let items = collect_after_limit(items, request.page.as_ref(), |item| {
            &item.sip_dispatch_rule_id
        });

        Ok(proto::ListSipDispatchRuleResponse { items })
    }

    /// Returns inbound SIP trunks matching the called number.
    pub fn select_sip_inbound_trunk(
        &self,
        called_number: &str,
    ) -> Result<Vec<proto::SipInboundTrunkInfo>, RoomStoreError> {
        let response = self.list_sip_inbound_trunk(&proto::ListSipInboundTrunkRequest {
            numbers: vec![called_number.to_string()],
            ..Default::default()
        })?;
        Ok(response.items)
    }

    /// Returns SIP dispatch rules matching a trunk ID.
    pub fn select_sip_dispatch_rule(
        &self,
        trunk_id: &str,
    ) -> Result<Vec<proto::SipDispatchRuleInfo>, RoomStoreError> {
        let request = if trunk_id.is_empty() {
            proto::ListSipDispatchRuleRequest::default()
        } else {
            proto::ListSipDispatchRuleRequest {
                trunk_ids: vec![trunk_id.to_string()],
                ..Default::default()
            }
        };

        let response = self.list_sip_dispatch_rule(&request)?;
        Ok(response.items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted_ids(mut ids: Vec<String>) -> Vec<String> {
        ids.sort();
        ids
    }

    // Upstream: livekit/pkg/service/ioservice_sip_test.go::TestSIPTrunkSelect
    #[test]
    fn sip_trunk_select_matches_upstream_behavior() {
        let store = RoomStore::default();

        for trunk in [
            proto::SipInboundTrunkInfo {
                sip_trunk_id: "any".to_string(),
                numbers: vec![],
                ..Default::default()
            },
            proto::SipInboundTrunkInfo {
                sip_trunk_id: "B".to_string(),
                numbers: vec!["B1".to_string(), "B2".to_string()],
                ..Default::default()
            },
            proto::SipInboundTrunkInfo {
                sip_trunk_id: "BC".to_string(),
                numbers: vec!["B1".to_string(), "C1".to_string()],
                ..Default::default()
            },
        ] {
            store
                .store_sip_inbound_trunk(&trunk)
                .expect("inbound trunk should store");
        }

        for trunk in [
            proto::SipTrunkInfo {
                sip_trunk_id: "old-any".to_string(),
                outbound_number: "".to_string(),
                ..Default::default()
            },
            proto::SipTrunkInfo {
                sip_trunk_id: "old-A".to_string(),
                outbound_number: "A".to_string(),
                ..Default::default()
            },
        ] {
            store
                .store_sip_trunk(&trunk)
                .expect("legacy trunk should store");
        }

        let cases = vec![
            ("A", vec!["old-A", "old-any", "any"]),
            ("B1", vec!["B", "BC", "old-any", "any"]),
            ("B2", vec!["B", "old-any", "any"]),
            ("C1", vec!["BC", "old-any", "any"]),
            ("wrong", vec!["old-any", "any"]),
        ];

        for (number, expected) in cases {
            let ids = store
                .select_sip_inbound_trunk(number)
                .expect("select should succeed")
                .into_iter()
                .map(|trunk| trunk.sip_trunk_id)
                .collect::<Vec<_>>();
            assert_eq!(
                sorted_ids(ids),
                sorted_ids(expected.into_iter().map(str::to_string).collect())
            );
        }
    }

    // Upstream: livekit/pkg/service/ioservice_sip_test.go::TestSIPRuleSelect
    #[test]
    fn sip_rule_select_matches_upstream_behavior() {
        let store = RoomStore::default();

        for rule in [
            proto::SipDispatchRuleInfo {
                sip_dispatch_rule_id: "any".to_string(),
                trunk_ids: vec![],
                ..Default::default()
            },
            proto::SipDispatchRuleInfo {
                sip_dispatch_rule_id: "B".to_string(),
                trunk_ids: vec!["B1".to_string(), "B2".to_string()],
                ..Default::default()
            },
            proto::SipDispatchRuleInfo {
                sip_dispatch_rule_id: "BC".to_string(),
                trunk_ids: vec!["B1".to_string(), "C1".to_string()],
                ..Default::default()
            },
        ] {
            store
                .store_sip_dispatch_rule(&rule)
                .expect("rule should store");
        }

        let cases = vec![
            ("A", vec!["any"]),
            ("B1", vec!["B", "BC", "any"]),
            ("B2", vec!["B", "any"]),
            ("C1", vec!["BC", "any"]),
            ("wrong", vec!["any"]),
        ];

        for (trunk, expected) in cases {
            let ids = store
                .select_sip_dispatch_rule(trunk)
                .expect("select should succeed")
                .into_iter()
                .map(|rule| rule.sip_dispatch_rule_id)
                .collect::<Vec<_>>();
            assert_eq!(
                sorted_ids(ids),
                sorted_ids(expected.into_iter().map(str::to_string).collect())
            );
        }
    }

    // Upstream: livekit/pkg/service/redisstore_sip_test.go::TestSIPStoreDispatch
    #[test]
    fn sip_store_dispatch_crud_matches_upstream_behavior() {
        let store = RoomStore::default();
        let id = "dispatch-1";

        let list = store
            .list_sip_dispatch_rule(&proto::ListSipDispatchRuleRequest::default())
            .expect("list should succeed");
        assert!(list.items.is_empty());

        assert_eq!(
            store.load_sip_dispatch_rule(id),
            Err(RoomStoreError::SipDispatchRuleNotFound)
        );

        let invalid_rule = proto::SipDispatchRuleInfo {
            trunk_ids: vec!["trunk".to_string()],
            ..Default::default()
        };
        assert!(matches!(
            store.store_sip_dispatch_rule(&invalid_rule),
            Err(RoomStoreError::InvalidArgument(_))
        ));

        let rule = proto::SipDispatchRuleInfo {
            sip_dispatch_rule_id: id.to_string(),
            trunk_ids: vec!["trunk".to_string()],
            ..Default::default()
        };
        store
            .store_sip_dispatch_rule(&rule)
            .expect("store should succeed");

        let loaded = store
            .load_sip_dispatch_rule(id)
            .expect("load should succeed");
        assert_eq!(loaded, rule);

        let list = store
            .list_sip_dispatch_rule(&proto::ListSipDispatchRuleRequest::default())
            .expect("list should succeed");
        assert_eq!(list.items, vec![rule.clone()]);

        store
            .delete_sip_dispatch_rule(id)
            .expect("delete should succeed");
        store
            .delete_sip_dispatch_rule(id)
            .expect("delete should be idempotent");

        let list = store
            .list_sip_dispatch_rule(&proto::ListSipDispatchRuleRequest::default())
            .expect("list should succeed");
        assert!(list.items.is_empty());

        assert_eq!(
            store.load_sip_dispatch_rule(id),
            Err(RoomStoreError::SipDispatchRuleNotFound)
        );
    }

    // Upstream: livekit/pkg/service/redisstore_sip_test.go::TestSIPStoreTrunk
    #[test]
    fn sip_store_trunk_crud_and_compat_matches_upstream_behavior() {
        let store = RoomStore::default();

        let old_id = "old-id";
        let in_id = "in-id";
        let out_id = "out-id";

        assert!(
            store
                .list_sip_trunk(&proto::ListSipTrunkRequest::default())
                .expect("legacy list should succeed")
                .items
                .is_empty()
        );
        assert!(
            store
                .list_sip_inbound_trunk(&proto::ListSipInboundTrunkRequest::default())
                .expect("inbound list should succeed")
                .items
                .is_empty()
        );
        assert!(
            store
                .list_sip_outbound_trunk(&proto::ListSipOutboundTrunkRequest::default())
                .expect("outbound list should succeed")
                .items
                .is_empty()
        );

        assert_eq!(
            store.load_sip_trunk(old_id),
            Err(RoomStoreError::SipTrunkNotFound)
        );
        assert_eq!(
            store.load_sip_inbound_trunk(old_id),
            Err(RoomStoreError::SipTrunkNotFound)
        );
        assert_eq!(
            store.load_sip_outbound_trunk(old_id),
            Err(RoomStoreError::SipTrunkNotFound)
        );

        assert!(matches!(
            store.store_sip_trunk(&proto::SipTrunkInfo {
                name: "Legacy".to_string(),
                ..Default::default()
            }),
            Err(RoomStoreError::InvalidArgument(_))
        ));
        assert!(matches!(
            store.store_sip_inbound_trunk(&proto::SipInboundTrunkInfo {
                name: "Inbound".to_string(),
                ..Default::default()
            }),
            Err(RoomStoreError::InvalidArgument(_))
        ));
        assert!(matches!(
            store.store_sip_outbound_trunk(&proto::SipOutboundTrunkInfo {
                name: "Outbound".to_string(),
                ..Default::default()
            }),
            Err(RoomStoreError::InvalidArgument(_))
        ));

        let old = proto::SipTrunkInfo {
            sip_trunk_id: old_id.to_string(),
            name: "Legacy".to_string(),
            ..Default::default()
        };
        let inbound = proto::SipInboundTrunkInfo {
            sip_trunk_id: in_id.to_string(),
            name: "Inbound".to_string(),
            ..Default::default()
        };
        let outbound = proto::SipOutboundTrunkInfo {
            sip_trunk_id: out_id.to_string(),
            name: "Outbound".to_string(),
            ..Default::default()
        };

        store
            .store_sip_trunk(&old)
            .expect("legacy store should succeed");
        store
            .store_sip_inbound_trunk(&inbound)
            .expect("inbound store should succeed");
        store
            .store_sip_outbound_trunk(&outbound)
            .expect("outbound store should succeed");

        assert_eq!(store.load_sip_trunk(old_id).expect("legacy load"), old);
        assert_eq!(
            store.load_sip_inbound_trunk(in_id).expect("inbound load"),
            inbound
        );
        assert_eq!(
            store
                .load_sip_outbound_trunk(out_id)
                .expect("outbound load"),
            outbound
        );

        assert_eq!(
            store.load_sip_trunk(in_id).expect("legacy compat inbound"),
            sip_inbound_to_legacy(&inbound)
        );
        assert_eq!(
            store
                .load_sip_trunk(out_id)
                .expect("legacy compat outbound"),
            sip_outbound_to_legacy(&outbound)
        );
        assert_eq!(
            store
                .load_sip_inbound_trunk(old_id)
                .expect("inbound compat legacy"),
            sip_trunk_to_inbound(&old)
        );
        assert_eq!(
            store
                .load_sip_outbound_trunk(old_id)
                .expect("outbound compat legacy"),
            sip_trunk_to_outbound(&old)
        );

        let mut legacy_list = store
            .list_sip_trunk(&proto::ListSipTrunkRequest::default())
            .expect("legacy list should succeed")
            .items;
        legacy_list.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(legacy_list.len(), 3);

        let mut inbound_list = store
            .list_sip_inbound_trunk(&proto::ListSipInboundTrunkRequest::default())
            .expect("inbound list should succeed")
            .items;
        inbound_list.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(inbound_list.len(), 2);

        let mut outbound_list = store
            .list_sip_outbound_trunk(&proto::ListSipOutboundTrunkRequest::default())
            .expect("outbound list should succeed")
            .items;
        outbound_list.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(outbound_list.len(), 2);

        store
            .delete_sip_trunk(old_id)
            .expect("delete legacy should succeed");
        store
            .delete_sip_trunk(old_id)
            .expect("delete legacy should be idempotent");

        assert_eq!(
            store
                .load_sip_inbound_trunk(in_id)
                .expect("inbound still exists"),
            inbound
        );
        assert_eq!(
            store
                .load_sip_outbound_trunk(out_id)
                .expect("outbound still exists"),
            outbound
        );

        store
            .delete_sip_trunk(in_id)
            .expect("delete inbound should succeed");
        store
            .delete_sip_trunk(out_id)
            .expect("delete outbound should succeed");

        assert!(
            store
                .list_sip_trunk(&proto::ListSipTrunkRequest::default())
                .expect("legacy list should succeed")
                .items
                .is_empty()
        );
        assert!(
            store
                .list_sip_inbound_trunk(&proto::ListSipInboundTrunkRequest::default())
                .expect("inbound list should succeed")
                .items
                .is_empty()
        );
        assert!(
            store
                .list_sip_outbound_trunk(&proto::ListSipOutboundTrunkRequest::default())
                .expect("outbound list should succeed")
                .items
                .is_empty()
        );

        assert_eq!(
            store.load_sip_trunk(old_id),
            Err(RoomStoreError::SipTrunkNotFound)
        );
        assert_eq!(
            store.load_sip_inbound_trunk(old_id),
            Err(RoomStoreError::SipTrunkNotFound)
        );
        assert_eq!(
            store.load_sip_outbound_trunk(old_id),
            Err(RoomStoreError::SipTrunkNotFound)
        );
    }

    // Upstream: livekit/pkg/service/redisstore_sip_test.go::TestSIPTrunkList
    #[test]
    fn sip_trunk_list_filters_and_pagination_match_upstream_behavior() {
        let store = RoomStore::default();

        let mut all_ids = Vec::new();
        for i in 0..250 {
            let id = format!("{i:05}");
            all_ids.push(id.clone());
            if id.ends_with('0') {
                store
                    .store_sip_trunk(&proto::SipTrunkInfo {
                        sip_trunk_id: id.clone(),
                        outbound_number: id,
                        ..Default::default()
                    })
                    .expect("legacy trunk should store");
            } else {
                store
                    .store_sip_inbound_trunk(&proto::SipInboundTrunkInfo {
                        sip_trunk_id: id.clone(),
                        numbers: vec![id],
                        ..Default::default()
                    })
                    .expect("inbound trunk should store");
            }
        }

        let got = store
            .list_sip_inbound_trunk(&proto::ListSipInboundTrunkRequest::default())
            .expect("list should succeed")
            .items
            .into_iter()
            .map(|item| item.sip_trunk_id)
            .collect::<Vec<_>>();
        assert_eq!(got, all_ids);

        let got = store
            .list_sip_inbound_trunk(&proto::ListSipInboundTrunkRequest {
                page: Some(proto::Pagination {
                    limit: 10,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect("list should succeed")
            .items
            .into_iter()
            .map(|item| item.sip_trunk_id)
            .collect::<Vec<_>>();
        assert_eq!(got, all_ids[..10]);

        let got = store
            .list_sip_inbound_trunk(&proto::ListSipInboundTrunkRequest {
                page: Some(proto::Pagination {
                    limit: 10,
                    after_id: all_ids[55].clone(),
                }),
                ..Default::default()
            })
            .expect("list should succeed")
            .items
            .into_iter()
            .map(|item| item.sip_trunk_id)
            .collect::<Vec<_>>();
        assert_eq!(got, all_ids[56..66]);

        let got = store
            .list_sip_inbound_trunk(&proto::ListSipInboundTrunkRequest {
                page: Some(proto::Pagination {
                    limit: 10,
                    after_id: all_ids[5].clone(),
                }),
                trunk_ids: vec![
                    all_ids[10].clone(),
                    all_ids[3].clone(),
                    "invalid".to_string(),
                    all_ids[8].clone(),
                ],
                ..Default::default()
            })
            .expect("list should succeed")
            .items
            .into_iter()
            .map(|item| item.sip_trunk_id)
            .collect::<Vec<_>>();
        assert_eq!(got, vec![all_ids[8].clone(), all_ids[10].clone()]);
    }

    // Upstream: livekit/pkg/service/redisstore_sip_test.go::TestSIPRuleList
    #[test]
    fn sip_rule_list_filters_and_pagination_match_upstream_behavior() {
        let store = RoomStore::default();

        let mut all_ids = Vec::new();
        for i in 0..250 {
            let id = format!("{i:05}");
            all_ids.push(id.clone());
            store
                .store_sip_dispatch_rule(&proto::SipDispatchRuleInfo {
                    sip_dispatch_rule_id: id.clone(),
                    trunk_ids: vec![id],
                    ..Default::default()
                })
                .expect("rule should store");
        }

        let got = store
            .list_sip_dispatch_rule(&proto::ListSipDispatchRuleRequest::default())
            .expect("list should succeed")
            .items
            .into_iter()
            .map(|item| item.sip_dispatch_rule_id)
            .collect::<Vec<_>>();
        assert_eq!(got, all_ids);

        let got = store
            .list_sip_dispatch_rule(&proto::ListSipDispatchRuleRequest {
                page: Some(proto::Pagination {
                    limit: 10,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect("list should succeed")
            .items
            .into_iter()
            .map(|item| item.sip_dispatch_rule_id)
            .collect::<Vec<_>>();
        assert_eq!(got, all_ids[..10]);

        let got = store
            .list_sip_dispatch_rule(&proto::ListSipDispatchRuleRequest {
                page: Some(proto::Pagination {
                    limit: 10,
                    after_id: all_ids[55].clone(),
                }),
                ..Default::default()
            })
            .expect("list should succeed")
            .items
            .into_iter()
            .map(|item| item.sip_dispatch_rule_id)
            .collect::<Vec<_>>();
        assert_eq!(got, all_ids[56..66]);

        let got = store
            .list_sip_dispatch_rule(&proto::ListSipDispatchRuleRequest {
                page: Some(proto::Pagination {
                    limit: 10,
                    after_id: all_ids[5].clone(),
                }),
                dispatch_rule_ids: vec![
                    all_ids[10].clone(),
                    all_ids[3].clone(),
                    "invalid".to_string(),
                    all_ids[8].clone(),
                ],
                ..Default::default()
            })
            .expect("list should succeed")
            .items
            .into_iter()
            .map(|item| item.sip_dispatch_rule_id)
            .collect::<Vec<_>>();
        assert_eq!(got, vec![all_ids[8].clone(), all_ids[10].clone()]);
    }
}
