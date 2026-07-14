use std::path::Path;

#[cfg(all(feature = "native-process", not(target_os = "wasi")))]
use std::process::Stdio;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

pub type BoxReader = Box<dyn AsyncRead + Send + Unpin>;
pub type BoxWriter = Box<dyn AsyncWrite + Send + Unpin>;

pub struct SpawnedProcess {
    pub stdin: BoxWriter,
    pub stdout: BoxReader,
    pub child: Box<dyn ChildHandle>,
}

#[async_trait]
pub trait ChildHandle: Send + Sync {
    async fn kill(&mut self) -> anyhow::Result<()>;
    async fn wait(&mut self) -> anyhow::Result<i32>;
    fn id(&self) -> Option<u32>;
}

#[async_trait]
pub trait ProcessBackend: Send + Sync {
    async fn spawn(
        &self,
        executable: &str,
        args: &[String],
        cwd: &Path,
    ) -> anyhow::Result<SpawnedProcess>;
}

#[cfg(all(feature = "native-process", not(target_os = "wasi")))]
#[derive(Debug, Default)]
pub struct NativeProcessBackend;

#[cfg(all(feature = "native-process", not(target_os = "wasi")))]
struct TokioChild(tokio::process::Child);

#[cfg(all(feature = "native-process", not(target_os = "wasi")))]
#[async_trait]
impl ChildHandle for TokioChild {
    async fn kill(&mut self) -> anyhow::Result<()> {
        self.0.kill().await?;
        Ok(())
    }

    async fn wait(&mut self) -> anyhow::Result<i32> {
        Ok(self.0.wait().await?.code().unwrap_or(128))
    }

    fn id(&self) -> Option<u32> {
        self.0.id()
    }
}

#[cfg(all(feature = "native-process", not(target_os = "wasi")))]
#[async_trait]
impl ProcessBackend for NativeProcessBackend {
    async fn spawn(
        &self,
        executable: &str,
        args: &[String],
        cwd: &Path,
    ) -> anyhow::Result<SpawnedProcess> {
        let mut child = tokio::process::Command::new(executable)
            .args(args)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("Pi child stdin was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Pi child stdout was not piped"))?;
        Ok(SpawnedProcess {
            stdin: Box::new(stdin),
            stdout: Box::new(stdout),
            child: Box::new(TokioChild(child)),
        })
    }
}

#[cfg(all(feature = "agentos-wasm", target_os = "wasi"))]
pub struct AgentOsWasiProcessBackend;

#[cfg(all(feature = "agentos-wasm", target_os = "wasi"))]
#[async_trait]
impl ProcessBackend for AgentOsWasiProcessBackend {
    async fn spawn(
        &self,
        executable: &str,
        args: &[String],
        cwd: &Path,
    ) -> anyhow::Result<SpawnedProcess> {
        wasi_backend::spawn(executable, args, cwd)
    }
}

#[cfg(all(feature = "agentos-wasm", target_os = "wasi"))]
pub fn agentos_stdio() -> anyhow::Result<(BoxReader, BoxWriter)> {
    wasi_backend::stdio()
}

#[cfg(all(feature = "agentos-wasm", target_os = "wasi"))]
mod wasi_backend {
    use std::{
        io::{self, Read, Write},
        mem::ManuallyDrop,
        os::fd::{FromRawFd, RawFd},
        path::Path,
        pin::Pin,
        task::{Context, Poll},
    };

    use async_trait::async_trait;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use super::{BoxReader, BoxWriter, ChildHandle, SpawnedProcess};

    const MAX_ARG_COUNT: usize = 4096;
    const MAX_SERIALIZED_BYTES: usize = 1024 * 1024;
    const MAX_CWD_BYTES: usize = 4096;

    #[link(wasm_import_module = "host_process")]
    unsafe extern "C" {
        fn proc_spawn(
            argv_ptr: *const u8,
            argv_len: u32,
            envp_ptr: *const u8,
            envp_len: u32,
            stdin_fd: u32,
            stdout_fd: u32,
            stderr_fd: u32,
            cwd_ptr: *const u8,
            cwd_len: u32,
            ret_pid: *mut u32,
        ) -> u32;
        fn proc_waitpid(pid: u32, options: u32, ret_status: *mut u32, ret_pid: *mut u32) -> u32;
        fn proc_kill(pid: u32, signal: u32) -> u32;
        fn fd_pipe(ret_read_fd: *mut u32, ret_write_fd: *mut u32) -> u32;
    }

    struct WasiReader {
        fd: RawFd,
        owned: bool,
    }

    struct WasiWriter {
        fd: RawFd,
        owned: bool,
    }

    unsafe impl Send for WasiReader {}
    unsafe impl Send for WasiWriter {}

    impl WasiReader {
        fn new(fd: RawFd, owned: bool) -> io::Result<Self> {
            set_nonblocking(fd)?;
            Ok(Self { fd, owned })
        }
    }

    impl WasiWriter {
        fn new(fd: RawFd, owned: bool) -> io::Result<Self> {
            Ok(Self { fd, owned })
        }
    }

    impl AsyncRead for WasiReader {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let file = unsafe {
                ManuallyDrop::new(std::fs::File::from_raw_fd(self.as_ref().get_ref().fd))
            };
            match (&*file).read(buffer.initialize_unfilled()) {
                Ok(read) => {
                    buffer.advance(read);
                    Poll::Ready(Ok(()))
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(error) => Poll::Ready(Err(error)),
            }
        }
    }

    impl AsyncWrite for WasiWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            let file = unsafe {
                ManuallyDrop::new(std::fs::File::from_raw_fd(self.as_ref().get_ref().fd))
            };
            match (&*file).write(buffer) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                result => Poll::Ready(result),
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl Drop for WasiReader {
        fn drop(&mut self) {
            if self.owned {
                let _ = unsafe { wasi::fd_close(self.fd as u32) };
            }
        }
    }

    impl Drop for WasiWriter {
        fn drop(&mut self) {
            if self.owned {
                let _ = unsafe { wasi::fd_close(self.fd as u32) };
            }
        }
    }

    struct WasiChild {
        pid: u32,
        reaped: bool,
    }

    unsafe impl Send for WasiChild {}
    unsafe impl Sync for WasiChild {}

    #[async_trait]
    impl ChildHandle for WasiChild {
        async fn kill(&mut self) -> anyhow::Result<()> {
            errno(unsafe { proc_kill(self.pid, 15) }, "proc_kill")?;
            Ok(())
        }

        async fn wait(&mut self) -> anyhow::Result<i32> {
            let mut status = 0;
            let mut actual_pid = 0;
            errno(
                unsafe { proc_waitpid(self.pid, 0, &mut status, &mut actual_pid) },
                "proc_waitpid",
            )?;
            self.reaped = true;
            Ok(status as i32)
        }

        fn id(&self) -> Option<u32> {
            Some(self.pid)
        }
    }

    impl Drop for WasiChild {
        fn drop(&mut self) {
            if self.reaped {
                return;
            }
            unsafe {
                let _ = proc_kill(self.pid, 9);
                let mut status = 0;
                let mut actual_pid = 0;
                let _ = proc_waitpid(self.pid, 0, &mut status, &mut actual_pid);
            }
        }
    }

    pub(super) fn stdio() -> anyhow::Result<(BoxReader, BoxWriter)> {
        Ok((
            Box::new(WasiReader::new(0, false)?),
            Box::new(WasiWriter::new(1, false)?),
        ))
    }

    pub(super) fn spawn(
        executable: &str,
        args: &[String],
        cwd: &Path,
    ) -> anyhow::Result<SpawnedProcess> {
        if args.len() + 1 > MAX_ARG_COUNT {
            anyhow::bail!("argument count exceeds limit of {MAX_ARG_COUNT}");
        }
        let cwd = cwd
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Pi cwd is not valid UTF-8"))?;
        if cwd.len() > MAX_CWD_BYTES || cwd.as_bytes().contains(&0) {
            anyhow::bail!("Pi cwd is invalid or exceeds {MAX_CWD_BYTES} bytes");
        }
        let mut argv = Vec::new();
        for item in std::iter::once(executable).chain(args.iter().map(String::as_str)) {
            if item.as_bytes().contains(&0) {
                anyhow::bail!("Pi argument contains NUL");
            }
            if !argv.is_empty() {
                argv.push(0);
            }
            argv.extend_from_slice(item.as_bytes());
            if argv.len() > MAX_SERIALIZED_BYTES {
                anyhow::bail!("serialized Pi arguments exceed {MAX_SERIALIZED_BYTES} bytes");
            }
        }

        let (stdin_read, stdin_write) = pipe()?;
        let (stdout_read, stdout_write) = match pipe() {
            Ok(pipe) => pipe,
            Err(error) => {
                close(stdin_read);
                close(stdin_write);
                return Err(error);
            }
        };
        let mut pid = 0;
        let result = errno(
            unsafe {
                proc_spawn(
                    argv.as_ptr(),
                    u32::try_from(argv.len())?,
                    std::ptr::null(),
                    0,
                    stdin_read,
                    stdout_write,
                    2,
                    cwd.as_ptr(),
                    u32::try_from(cwd.len())?,
                    &mut pid,
                )
            },
            "proc_spawn",
        );
        close(stdin_read);
        close(stdout_write);
        if let Err(error) = result {
            close(stdin_write);
            close(stdout_read);
            return Err(error);
        }

        Ok(SpawnedProcess {
            stdin: Box::new(WasiWriter::new(stdin_write as RawFd, true)?),
            stdout: Box::new(WasiReader::new(stdout_read as RawFd, true)?),
            child: Box::new(WasiChild { pid, reaped: false }),
        })
    }

    fn pipe() -> anyhow::Result<(u32, u32)> {
        let mut read = 0;
        let mut write = 0;
        errno(unsafe { fd_pipe(&mut read, &mut write) }, "fd_pipe")?;
        Ok((read, write))
    }

    fn close(fd: u32) {
        let _ = unsafe { wasi::fd_close(fd) };
    }

    fn set_nonblocking(fd: RawFd) -> io::Result<()> {
        unsafe { wasi::fd_fdstat_set_flags(fd as u32, wasi::FDFLAGS_NONBLOCK) }
            .map_err(|error| io::Error::other(format!("wasi errno {}", error.raw())))
    }

    fn errno(value: u32, operation: &str) -> anyhow::Result<()> {
        if value == 0 {
            Ok(())
        } else {
            anyhow::bail!("AgentOS {operation} failed with WASI errno {value}")
        }
    }
}
