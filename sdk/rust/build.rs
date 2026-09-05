use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn watch_go(path: &Path) {
    if path.is_file() {
        println!("cargo:rerun-if-changed={}", path.display());
    } else if let Ok(entries) = fs::read_dir(path) {
        println!("cargo:rerun-if-changed={}", path.display());
        for entry in entries.flatten() {
            watch_go(&entry.path());
        }
    }
}

fn main() {
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    // docs.rs builds Rust documentation only and cannot supply the Go/C toolchain.
    // Do not manufacture a library: ordinary builds always follow the path below.
    if env::var_os("DOCS_RS").is_some() {
        return;
    }
    let target = env::var("TARGET").expect("Cargo must set TARGET");
    let host = env::var("HOST").expect("Cargo must set HOST");
    let os = env::var("CARGO_CFG_TARGET_OS").expect("Cargo must set target OS");
    let env_name = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    if !(os == "macos" || (os == "linux" && env_name == "gnu")) {
        panic!("rhiza supports only macOS or linux-gnu targets; target is {target}. Cross-compilation is not configured.");
    }
    if target != host {
        panic!("rhiza native builds support only the host target ({host}); cross-compilation for {target} is not configured.");
    }
    let go_arch = if target.starts_with("aarch64-") {
        "arm64"
    } else if target.starts_with("x86_64-") {
        "amd64"
    } else {
        panic!("rhiza does not map Rust target architecture in {target} to Go");
    };
    let go_os = if os == "macos" { "darwin" } else { "linux" };

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    // A development checkout must use the adjacent source, even when a previous
    // packaging run left a native snapshot behind. Published crates have no such
    // module and therefore use their bundled snapshot.
    let native = manifest_dir.join("native");
    let adjacent = manifest_dir.join("../..");
    let adjacent_go_mod = adjacent.join("go.mod");
    println!("cargo:rerun-if-changed={}", adjacent_go_mod.display());
    watch_go(&adjacent.join("cmd/rhiza-ffi"));
    let repo = if adjacent.join("cmd/rhiza-ffi").is_dir()
        && fs::read_to_string(&adjacent_go_mod)
            .map(|go_mod| go_mod.contains("module github.com/mrchypark/rhiza"))
            .unwrap_or(false)
    {
        adjacent
    } else {
        native
    };
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let library_dir = match env::var_os("RHIZA_NATIVE_LIB_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => {
            let archive = out.join("librhiza_ffi.a");
            let status = Command::new("go")
                .current_dir(&repo)
                .env("CGO_ENABLED", "1")
                .env("GOWORK", "off")
                .env("GOOS", go_os)
                .env("GOARCH", go_arch)
                .args(["build", "-buildmode=c-archive", "-o"])
                .arg(&archive)
                .arg("./cmd/rhiza-ffi")
                .status()
                .expect("failed to start Go; install Go or set RHIZA_NATIVE_LIB_DIR to a directory containing librhiza_ffi.a");
            if !status.success() {
                panic!("building the Rhiza native archive failed; set RHIZA_NATIVE_LIB_DIR to a compatible prebuilt archive to avoid the local Go build");
            }
            out
        }
    };
    if !library_dir.join("librhiza_ffi.a").is_file() {
        panic!("RHIZA_NATIVE_LIB_DIR must contain librhiza_ffi.a");
    }
    if env::var_os("RHIZA_NATIVE_LIB_DIR").is_some() {
        println!(
            "cargo:rerun-if-changed={}",
            library_dir.join("librhiza_ffi.a").display()
        );
    }
    println!("cargo:rustc-link-search=native={}", library_dir.display());
    println!("cargo:rustc-link-lib=static=rhiza_ffi");
    if os == "linux" {
        for lib in ["pthread", "dl", "m", "resolv"] {
            println!("cargo:rustc-link-lib={lib}");
        }
    } else {
        println!("cargo:rustc-link-lib=resolv");
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=framework=Security");
    }
    for path in [
        "go.mod",
        "go.sum",
        "rhiza.go",
        "replica.go",
        "cmd/rhiza-ffi",
        "pkg",
        "internal",
    ] {
        watch_go(&repo.join(path));
    }
    println!("cargo:rerun-if-env-changed=RHIZA_NATIVE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=GOEXPERIMENT");
    println!("cargo:rerun-if-env-changed=GOFLAGS");
    println!("cargo:rerun-if-env-changed=CC");
    println!("cargo:rerun-if-env-changed=CGO_CFLAGS");
    println!("cargo:rerun-if-env-changed=CGO_CPPFLAGS");
    println!("cargo:rerun-if-env-changed=CGO_CXXFLAGS");
    println!("cargo:rerun-if-env-changed=CGO_LDFLAGS");
    println!("cargo:rerun-if-env-changed=GOTOOLCHAIN");
    println!("cargo:rerun-if-env-changed=GOROOT");
    println!("cargo:rerun-if-env-changed=SDKROOT");
    println!("cargo:rerun-if-env-changed=MACOSX_DEPLOYMENT_TARGET");
    println!("cargo:rerun-if-env-changed=GOOS");
    println!("cargo:rerun-if-env-changed=GOARCH");
    println!("cargo:rerun-if-env-changed=GOWORK");
}
