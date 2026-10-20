//! `wendelon-key`: the offline custody tool for Root2 role signing keys. All logic lives in the
//! library; this entry point only hands it the process's arguments.
#![forbid(unsafe_code)]

fn main() -> std::process::ExitCode {
	wendelon_key::main_entry()
}
