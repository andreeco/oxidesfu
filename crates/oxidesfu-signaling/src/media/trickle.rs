#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn is_final_trickle_candidate(candidate_init: &str) -> bool {
    candidate_init.is_empty()
}
