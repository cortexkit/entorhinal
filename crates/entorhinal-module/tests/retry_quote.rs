#[path = "../src/retry_quote.rs"]
mod retry_quote;

#[test]
fn retry_arguments_survive_the_native_shell() {
    let words = ["a b", "a'b", "a\"b", "--request-key", "key ' with space"];
    let binary = env!("CARGO_BIN_EXE_ckdev-argv-echo");
    let mut command = if cfg!(windows) {
        let mut command = std::process::Command::new("pwsh");
        command.args(["-NoProfile", "-Command"]);
        command.arg(format!(
            "& {} {}",
            retry_quote::powershell_word(binary),
            retry_quote::render_arguments(&words, true)
        ));
        command
    } else {
        let mut command = std::process::Command::new("sh");
        command.args([
            "-c",
            &format!(
                "{} {}",
                retry_quote::posix_word(binary),
                retry_quote::render_arguments(&words, false)
            ),
        ]);
        command
    };
    let output = command.output().expect("run native shell");
    assert!(output.status.success(), "{output:?}");
    let received: Vec<String> = serde_json::from_slice(&output.stdout).expect("argv JSON");
    assert_eq!(received, words);
}
