//! RSS and cgroup pressure. A soft signal skips a busy child. A hard signal does not.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainCause {
    Shutdown,
    MemorySoft,
    MemoryRatio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillCause {
    MemoryHard,
    MemoryRatio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedSignal {
    Drain(u32, DrainCause),
    Kill(u32, KillCause),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryAction {
    None,
    Drain,
    Kill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteAction {
    None,
    Drain,
    Kill,
}

#[derive(Debug, Default)]
pub struct PressureCounters {
    pub drains: u64,
    pub kills: u64,
}

pub fn memory_action(current: u64, limit: u64, soft: f64, hard: f64) -> MemoryAction {
    if limit == 0 || current == 0 {
        return MemoryAction::None;
    }
    if !(soft.is_finite() && hard.is_finite() && soft > 0.0 && hard > 0.0 && hard < 1.0) {
        return MemoryAction::None;
    }
    let soft = if soft < hard { soft } else { hard * 0.8 };
    let hard_bytes = threshold(limit, hard);
    let soft_bytes = threshold(limit, soft);
    if hard_bytes == 0 {
        MemoryAction::None
    } else if current >= hard_bytes {
        MemoryAction::Kill
    } else if soft_bytes > 0 && current >= soft_bytes {
        MemoryAction::Drain
    } else {
        MemoryAction::None
    }
}

fn threshold(limit: u64, ratio: f64) -> u64 {
    let millis = (ratio * 1000.0).round() as u64;
    limit.saturating_mul(millis) / 1000
}

pub fn byte_action(rss: u64, soft: Option<u64>, hard: Option<u64>) -> ByteAction {
    if hard.is_some_and(|limit| rss >= limit) {
        ByteAction::Kill
    } else if soft.is_some_and(|limit| rss >= limit) {
        ByteAction::Drain
    } else {
        ByteAction::None
    }
}

pub fn largest_child_to_kill(children: &[(u32, u64)], current: u64) -> Option<u32> {
    let (pid, rss) = children.iter().copied().max_by_key(|(_, rss)| *rss)?;
    if rss > 0 && rss.saturating_mul(2) >= current { Some(pid) } else { None }
}

pub fn plan_memory_signals(
    children: &[(u32, u64)],
    soft: Option<u64>,
    hard: Option<u64>,
    occupied: Option<u64>,
    limit: Option<u64>,
    soft_ratio: f64,
    hard_ratio: f64,
    already_drained: &[u32],
    busy: &[u32],
) -> Vec<PlannedSignal> {
    let mut planned = Vec::new();
    for &(pid, rss) in children {
        match byte_action(rss, soft, hard) {
            ByteAction::Kill => planned.push(PlannedSignal::Kill(pid, KillCause::MemoryHard)),
            ByteAction::Drain if !already_drained.contains(&pid) && !busy.contains(&pid) => {
                planned.push(PlannedSignal::Drain(pid, DrainCause::MemorySoft));
            }
            ByteAction::Drain | ByteAction::None => {}
        }
    }
    let (Some(occupied), Some(limit)) = (occupied, limit) else {
        return planned;
    };
    match memory_action(occupied, limit, soft_ratio, hard_ratio) {
        MemoryAction::None => {}
        MemoryAction::Kill => {
            if let Some(pid) = largest_child_to_kill(children, occupied) {
                if !planned.iter().any(|signal| matches!(signal, PlannedSignal::Kill(current, _) if *current == pid)) {
                    planned.retain(|signal| !matches!(signal, PlannedSignal::Drain(current, _) if *current == pid));
                    planned.push(PlannedSignal::Kill(pid, KillCause::MemoryRatio));
                }
            } else if let Some(pid) = ratio_drain(children, already_drained, busy, &planned) {
                planned.push(PlannedSignal::Drain(pid, DrainCause::MemoryRatio));
            }
        }
        MemoryAction::Drain => {
            if let Some(pid) = ratio_drain(children, already_drained, busy, &planned) {
                planned.push(PlannedSignal::Drain(pid, DrainCause::MemoryRatio));
            }
        }
    }
    planned
}

fn ratio_drain(children: &[(u32, u64)], already_drained: &[u32], busy: &[u32], planned: &[PlannedSignal]) -> Option<u32> {
    let killing = |pid: u32| planned.iter().any(|signal| matches!(signal, PlannedSignal::Kill(current, _) if *current == pid));
    let draining = |pid: u32| planned.iter().any(|signal| matches!(signal, PlannedSignal::Drain(current, _) if *current == pid));
    let (pid, _) = children
        .iter()
        .copied()
        .filter(|(pid, _)| !already_drained.contains(pid) && !busy.contains(pid) && !killing(*pid))
        .max_by_key(|(_, rss)| *rss)?;
    if draining(pid) { None } else { Some(pid) }
}

pub fn apply_planned(
    planned: &[PlannedSignal],
    drained: &mut std::collections::HashSet<u32>,
    mut signal: impl FnMut(u32, i32) -> bool,
    counters: &mut PressureCounters,
) {
    for item in planned {
        match *item {
            PlannedSignal::Kill(pid, _) => {
                if signal(pid, libc::SIGKILL) {
                    counters.kills = counters.kills.saturating_add(1);
                    drained.remove(&pid);
                }
            }
            PlannedSignal::Drain(pid, _) => {
                if drained.contains(&pid) {
                    continue;
                }
                if signal(pid, libc::SIGUSR1) {
                    drained.insert(pid);
                    counters.drains = counters.drains.saturating_add(1);
                }
            }
        }
    }
}

pub fn parse_memory_max(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if raw.is_empty() || raw == "max" { None } else { raw.parse::<u64>().ok().filter(|value| *value > 0) }
}

pub fn cgroup_memory_dir(cgroup_text: &str) -> Option<PathBuf> {
    for line in cgroup_text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("0::") {
            let rest = rest.trim_start_matches('/');
            return Some(if rest.is_empty() {
                PathBuf::from("/sys/fs/cgroup")
            } else {
                Path::new("/sys/fs/cgroup").join(rest)
            });
        }
    }
    None
}

pub fn inactive_file_bytes(stat: &str) -> Option<u64> {
    for line in stat.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() == Some("inactive_file") {
            return parts.next()?.parse().ok();
        }
    }
    None
}

pub fn working_set(current: u64, inactive_file: Option<u64>) -> u64 {
    match inactive_file {
        Some(inactive) => current.saturating_sub(inactive),
        None => current,
    }
}

pub fn hard_budget_exceeds_cgroup(processes: u32, hard: u64, memory_max: u64) -> bool {
    (processes as u64).saturating_mul(hard) > memory_max
}

pub fn rss_of(children: &[(u32, u64)], pid: u32) -> u64 {
    children.iter().find(|(id, _)| *id == pid).map(|(_, rss)| *rss).unwrap_or(0)
}

pub fn assemble_memory(max_raw: &str, current_raw: &str, stat: Option<&str>) -> Option<(u64, u64)> {
    let max = parse_memory_max(max_raw)?;
    let current: u64 = current_raw.trim().parse().ok()?;
    let inactive = stat.and_then(inactive_file_bytes);
    Some((working_set(current, inactive), max))
}

pub fn load_cgroup_text(cgroup_text: &str, mut read: impl FnMut(&Path) -> Option<String>) -> Option<(u64, u64)> {
    let dir = cgroup_memory_dir(cgroup_text)?;
    let max_raw = read(&dir.join("memory.max"))?;
    let current_raw = read(&dir.join("memory.current"))?;
    let stat = read(&dir.join("memory.stat"));
    assemble_memory(&max_raw, &current_raw, stat.as_deref())
}

pub fn container_memory() -> Option<(u64, u64)> {
    let text = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    load_cgroup_text(&text, |path| std::fs::read_to_string(path).ok())
}

pub fn resident_bytes(pid: u32) -> Option<u64> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    let pages: u64 = text.split_whitespace().nth(1)?.parse().ok()?;
    let page = page_size() as u64;
    Some(pages.saturating_mul(page))
}

fn page_size() -> usize {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page > 0 { page as usize } else { 4096 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::osutil::{process_alive, signal_group};
    use std::collections::HashSet;
    use std::process::Command;

    #[test]
    fn ratios_drain_then_kill_and_ignore_bad_input() {
        assert_eq!(memory_action(0, 100, 0.5, 0.9), MemoryAction::None);
        assert_eq!(memory_action(10, 0, 0.5, 0.9), MemoryAction::None);
        assert_eq!(memory_action(10, 100, f64::NAN, 0.9), MemoryAction::None);
        assert_eq!(memory_action(10, 100, 0.5, 1.0), MemoryAction::None);
        assert_eq!(memory_action(50, 100, 0.4, 0.8), MemoryAction::Drain);
        assert_eq!(memory_action(90, 100, 0.4, 0.8), MemoryAction::Kill);
        assert_eq!(memory_action(10, 100, 0.4, 0.8), MemoryAction::None);
        // soft above hard is pulled down so a drain still exists below the kill
        assert_eq!(memory_action(70, 100, 0.95, 0.8), MemoryAction::Drain);
        assert_eq!(memory_action(1, 1_000_000, 0.000_000_1, 0.5), MemoryAction::None);
        assert_eq!(memory_action(10, 100, 0.0001, 0.0002), MemoryAction::None);
    }

    #[test]
    fn byte_action_is_per_pid_and_hard_wins() {
        assert_eq!(byte_action(10, Some(20), Some(30)), ByteAction::None);
        assert_eq!(byte_action(20, Some(20), Some(30)), ByteAction::Drain);
        assert_eq!(byte_action(30, Some(20), Some(30)), ByteAction::Kill);
        assert_eq!(byte_action(5, None, None), ByteAction::None);
        assert_eq!(byte_action(5, None, Some(4)), ByteAction::Kill);
    }

    #[test]
    fn largest_child_must_own_half_the_usage() {
        assert_eq!(largest_child_to_kill(&[], 10), None);
        assert_eq!(largest_child_to_kill(&[(1, 0), (2, 0)], 10), None);
        assert_eq!(largest_child_to_kill(&[(1, 4), (2, 6)], 20), None);
        assert_eq!(largest_child_to_kill(&[(1, 4), (2, 10)], 20), Some(2));
        assert_eq!(rss_of(&[(1, 4), (2, 10)], 2), 10);
        assert_eq!(rss_of(&[(1, 4)], 9), 0);
    }

    #[test]
    fn soft_signal_skips_a_busy_child_and_hard_does_not() {
        let children = [(1, 100), (2, 80)];
        let soft = plan_memory_signals(&children, Some(50), Some(1_000), None, None, 0.8, 0.95, &[], &[1]);
        assert_eq!(soft, vec![PlannedSignal::Drain(2, DrainCause::MemorySoft)]);
        let hard = plan_memory_signals(&children, Some(50), Some(90), None, None, 0.8, 0.95, &[], &[1]);
        assert!(hard.contains(&PlannedSignal::Kill(1, KillCause::MemoryHard)));
        assert!(hard.contains(&PlannedSignal::Drain(2, DrainCause::MemorySoft)));
        let again = plan_memory_signals(&children, Some(50), None, None, None, 0.8, 0.95, &[1, 2], &[]);
        assert!(again.is_empty());
        let only_second = plan_memory_signals(&children, Some(50), None, None, None, 0.8, 0.95, &[2], &[]);
        assert_eq!(only_second, vec![PlannedSignal::Drain(1, DrainCause::MemorySoft)]);
    }

    #[test]
    fn cgroup_ratio_drains_one_free_child_or_kills_the_owner() {
        let children = [(1, 100), (2, 40)];
        let soft = plan_memory_signals(&children, None, None, Some(80), Some(100), 0.5, 0.9, &[], &[]);
        assert_eq!(soft, vec![PlannedSignal::Drain(1, DrainCause::MemoryRatio)]);
        let busy = plan_memory_signals(&children, None, None, Some(80), Some(100), 0.5, 0.9, &[], &[1]);
        assert_eq!(busy, vec![PlannedSignal::Drain(2, DrainCause::MemoryRatio)]);
        let none = plan_memory_signals(&children, None, None, Some(80), Some(100), 0.5, 0.9, &[], &[1, 2]);
        assert!(none.is_empty());
        let already = plan_memory_signals(&children, None, None, Some(80), Some(100), 0.5, 0.9, &[1], &[]);
        assert_eq!(already, vec![PlannedSignal::Drain(2, DrainCause::MemoryRatio)]);
        // byte drain of the largest suppresses a second ratio drain in the same plan
        let held = plan_memory_signals(&children, Some(10), None, Some(80), Some(100), 0.5, 0.9, &[], &[]);
        assert_eq!(
            held,
            vec![
                PlannedSignal::Drain(1, DrainCause::MemorySoft),
                PlannedSignal::Drain(2, DrainCause::MemorySoft),
            ]
        );
        let kill = plan_memory_signals(&children, Some(10), None, Some(150), Some(100), 0.5, 0.9, &[], &[]);
        assert!(kill.contains(&PlannedSignal::Kill(1, KillCause::MemoryRatio)));
        assert!(!kill.iter().any(|signal| matches!(signal, PlannedSignal::Drain(1, _))));
        let small = plan_memory_signals(&[(1, 10)], None, None, Some(95), Some(100), 0.5, 0.9, &[], &[]);
        assert_eq!(small, vec![PlannedSignal::Drain(1, DrainCause::MemoryRatio)]);
        let owned = plan_memory_signals(&[(1, 80)], None, None, Some(95), Some(100), 0.5, 0.9, &[], &[]);
        assert_eq!(owned, vec![PlannedSignal::Kill(1, KillCause::MemoryRatio)]);
        let duplicate = plan_memory_signals(&[(1, 80)], Some(10), Some(10), Some(95), Some(100), 0.5, 0.9, &[], &[]);
        assert_eq!(duplicate, vec![PlannedSignal::Kill(1, KillCause::MemoryHard)]);
    }

    #[test]
    fn apply_skips_a_failed_signal_and_a_repeat_drain() {
        let mut drained = HashSet::new();
        let mut counters = PressureCounters::default();
        let planned = vec![PlannedSignal::Drain(7, DrainCause::MemorySoft), PlannedSignal::Kill(8, KillCause::MemoryHard)];
        apply_planned(&planned, &mut drained, |_pid, _sig| false, &mut counters);
        assert_eq!(counters.drains, 0);
        assert!(drained.is_empty());
        apply_planned(&planned, &mut drained, |_pid, _sig| true, &mut counters);
        assert_eq!(counters.drains, 1);
        assert_eq!(counters.kills, 1);
        assert!(!drained.contains(&8));
        apply_planned(
            &[PlannedSignal::Drain(7, DrainCause::Shutdown)],
            &mut drained,
            |_pid, _sig| true,
            &mut counters,
        );
        assert_eq!(counters.drains, 1);
    }

    #[test]
    fn a_soft_signal_does_not_kill_a_busy_sleep_and_hard_does() {
        let mut child = Command::new("/bin/sleep");
        child.arg("30");
        let mut child = child.spawn().expect("sleep");
        let pid = child.id();
        let soft = plan_memory_signals(&[(pid, 1_000)], Some(10), Some(10_000), None, None, 0.8, 0.95, &[], &[pid]);
        assert!(soft.is_empty());
        let mut drained = HashSet::new();
        let mut counters = PressureCounters::default();
        apply_planned(&soft, &mut drained, signal_group, &mut counters);
        assert!(process_alive(pid));
        let hard = plan_memory_signals(&[(pid, 1_000)], Some(10), Some(100), None, None, 0.8, 0.95, &[], &[pid]);
        assert_eq!(hard, vec![PlannedSignal::Kill(pid, KillCause::MemoryHard)]);
        apply_planned(&hard, &mut drained, signal_group, &mut counters);
        let _ = child.wait();
        assert!(!process_alive(pid));
        assert_eq!(counters.kills, 1);
    }

    #[test]
    fn cgroup_text_and_rss_readers() {
        assert_eq!(parse_memory_max("max"), None);
        assert_eq!(parse_memory_max("  "), None);
        assert_eq!(parse_memory_max("0"), None);
        assert_eq!(parse_memory_max("nope"), None);
        assert_eq!(parse_memory_max(" 42 "), Some(42));
        assert_eq!(cgroup_memory_dir("1:name=systemd:/\n"), None);
        assert_eq!(cgroup_memory_dir("0::/\n").unwrap(), PathBuf::from("/sys/fs/cgroup"));
        assert_eq!(
            cgroup_memory_dir("0::/system.slice/worker\n").unwrap(),
            PathBuf::from("/sys/fs/cgroup/system.slice/worker")
        );
        assert_eq!(inactive_file_bytes("anon 1\ninactive_file 9\n"), Some(9));
        assert_eq!(inactive_file_bytes("inactive_file nope"), None);
        assert_eq!(inactive_file_bytes("anon 1"), None);
        assert_eq!(working_set(10, Some(3)), 7);
        assert_eq!(working_set(2, Some(5)), 0);
        assert_eq!(working_set(2, None), 2);
        assert!(hard_budget_exceeds_cgroup(3, 10, 20));
        assert!(!hard_budget_exceeds_cgroup(2, 10, 20));
        assert_eq!(assemble_memory("max", "1", None), None);
        assert_eq!(assemble_memory("10", "nope", None), None);
        assert_eq!(assemble_memory("10", "8", Some("inactive_file 3")), Some((5, 10)));
        assert_eq!(assemble_memory("10", "8", None), Some((8, 10)));
        assert_eq!(load_cgroup_text("nope", |_| Some("1".into())), None);
        assert_eq!(load_cgroup_text("0::/slot", |_| None), None);
        let loaded = load_cgroup_text("0::/slot", |path| {
            let name = path.file_name().and_then(|text| text.to_str()).unwrap_or("");
            match name {
                "memory.max" => Some("100".into()),
                "memory.current" => Some("40".into()),
                "memory.stat" => Some("inactive_file 10".into()),
                _ => None,
            }
        });
        assert_eq!(loaded, Some((30, 100)));
        let live = container_memory();
        assert!(live.is_some(), "this host publishes a cgroup memory max");
        let (usage, limit) = live.unwrap();
        assert!(limit > 0);
        assert!(usage <= limit);
        let self_rss = resident_bytes(std::process::id()).unwrap();
        assert!(self_rss > 0);
        assert!(resident_bytes(u32::MAX).is_none());
    }
}
