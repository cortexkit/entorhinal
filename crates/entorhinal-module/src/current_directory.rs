//! Read the operator's working directory, refusing errors instead of claiming
//! that requests originated at a fabricated filesystem root.

#[derive(Debug)]
pub(crate) struct CwdError {
    pub code: &'static str,
    pub message: String,
}

pub(crate) type CwdReader = fn() -> std::io::Result<std::path::PathBuf>;

pub(crate) fn read_current_directory(read: CwdReader) -> Result<String, CwdError> {
    let path = read().map_err(|error| CwdError {
        code: "cwd_unreadable",
        message: error.to_string(),
    })?;
    path.into_os_string()
        .into_string()
        .map_err(|path| CwdError {
            code: "path_not_unicode",
            message: format!(
                "current directory is not Unicode: {}",
                path.to_string_lossy()
            ),
        })
}
