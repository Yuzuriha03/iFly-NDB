fn main() {
    println!("cargo:rerun-if-changed=icon.ico");

    if cfg!(target_os = "windows") {
        let mut resource = winresource::WindowsResource::new();
        resource.set_icon("icon.ico");
        resource
            .compile()
            .expect("failed to compile Windows resources");
    }
}