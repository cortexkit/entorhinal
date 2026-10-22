//! A receiver whose cleanup call is refused waits before asking again.
use super::*;

fn abort_calls(mac: &Mac) -> usize {
    mac.discovery.peers["pi"]
        .calls()
        .iter()
        .filter(|r| r.op().method() == "agent_data.abort")
        .count()
}

/// The participant refuses abort eleven times, then accepts. Each refusal
/// waits 1 s, doubling to the 5-minute cap, before the next call; a driver
/// started afresh keeps the saved wait; the twelfth call finishes the abort.
#[tokio::test(start_paused = true)]
async fn refused_cleanup_backs_off_doubling_to_the_cap_then_aborts() {
    let f = Pair::new(&[("pi", "pi-session")]).await;
    let cut = f.published("backoff").await;
    f.b.pickup().await;
    f.b.abort(&cut);
    for _ in 0..11 {
        f.b.refuse("pi", "agent_data.abort", "invalid_request");
    }
    let start = clock();
    let mut calls = Vec::new();
    let mut driver = f.b.receiver(&cut);
    for second in 0..1300 {
        if f.b.row(&cut).step == MoveStep::Aborted {
            break;
        }
        let before = abort_calls(&f.b);
        drive(driver.tick()).await.unwrap();
        for _ in before..abort_calls(&f.b) {
            calls.push(clock() - start);
        }
        if second == 20 {
            // A restarted driver reads the saved wait instead of starting
            // again from zero.
            driver = f.b.receiver(&cut);
        }
        elapse(1000);
    }
    let gaps: Vec<i64> = calls.windows(2).map(|w| w[1] - w[0]).collect();
    assert_eq!(
        gaps,
        vec![
            1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 300_000, 300_000
        ],
        "abort calls at {calls:?}"
    );
    assert_eq!(f.b.row(&cut).step, MoveStep::Aborted);
    assert_eq!(f.a.record(&cut).await.state, MoveState::Aborted);
}
