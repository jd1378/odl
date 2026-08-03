//! Spawning and tearing down a helper process together with its children.
//!
//! `yt-dlp` spawns `ffmpeg` to mux formats, so killing the process we started
//! is not enough — a cancelled download would leave `ffmpeg` running and still
//! writing into odl's download directory. Both platforms therefore group the
//! child with its descendants and signal the group as a unit: a process group
//! on unix, a Job Object on Windows.

use std::io;
use std::process::ExitStatus;
use std::time::Duration;
use tokio::process::{Child, ChildStderr, ChildStdout, Command};

/// How long a helper gets to exit on its own after a polite terminate before
/// it is killed outright. Long enough for `ffmpeg` to close its output file,
/// short enough not to stall a cancelling user.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(2);

/// Groups of helper processes currently alive, so a caller that must exit
/// without unwinding can still take them down.
///
/// `Drop` is the normal cleanup path, but it never runs on
/// [`std::process::exit`], which is exactly what a user pressing Ctrl-C twice
/// asks for. Without this, that second press would leave a downloader — and
/// whatever it spawned — running with nothing left to stop it.
static LIVE_GROUPS: std::sync::Mutex<Vec<GroupHandle>> = std::sync::Mutex::new(Vec::new());

/// Identifier for a whole group of processes, as this platform names one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GroupHandle(#[cfg(unix)] i32, #[cfg(windows)] usize);

// SAFETY (windows): the payload is an opaque kernel handle, valid in any
// thread of the process; every use goes through the Win32 API.
#[cfg(windows)]
unsafe impl Send for GroupHandle {}

fn register_group(handle: GroupHandle) {
    if let Ok(mut live) = LIVE_GROUPS.lock() {
        live.push(handle);
    }
}

fn forget_group(handle: GroupHandle) {
    if let Ok(mut live) = LIVE_GROUPS.lock() {
        live.retain(|h| *h != handle);
    }
}

/// Kill every helper group still running, without waiting for any of them.
///
/// For the path where the process is about to exit regardless: the point is
/// that nothing outlives odl, not that it shuts down tidily.
pub fn kill_all_groups() {
    let Ok(mut live) = LIVE_GROUPS.lock() else {
        return;
    };
    for handle in live.drain(..) {
        #[cfg(unix)]
        unix_impl::signal_pgid(handle.0, unix_impl::SIGKILL);
        #[cfg(windows)]
        windows_impl::terminate_job(handle.0);
    }
}

/// A spawned helper process whose descendants die with it.
#[derive(Debug)]
pub struct ManagedChild {
    child: Child,
    group: Option<GroupHandle>,
    #[cfg(windows)]
    job: windows_impl::JobObject,
}

impl ManagedChild {
    /// Spawn `cmd` in its own process group / job.
    ///
    /// The command's stdio configuration is left to the caller; only the
    /// grouping is applied here.
    pub fn spawn(cmd: &mut Command) -> io::Result<Self> {
        #[cfg(unix)]
        {
            unix_impl::prepare(cmd);
            let child = cmd.spawn()?;
            // The child leads its own group, so its pid names the group.
            let group = child
                .id()
                .and_then(|p| i32::try_from(p).ok())
                .map(GroupHandle);
            if let Some(g) = group {
                register_group(g);
            }
            Ok(Self { child, group })
        }
        #[cfg(windows)]
        {
            windows_impl::prepare(cmd);
            let job = windows_impl::JobObject::create()?;
            let child = cmd.spawn()?;
            job.assign(&child)?;
            let group = GroupHandle(job.raw());
            register_group(group);
            Ok(Self {
                child,
                group: Some(group),
                job,
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let child = cmd.spawn()?;
            Ok(Self { child, group: None })
        }
    }

    /// Take the child's stdout pipe, if it was configured as one.
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    /// Take the child's stderr pipe, if it was configured as one.
    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }

    /// Wait for the child to exit.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        let status = self.child.wait().await;
        if status.is_ok() {
            self.release_group();
        }
        status
    }

    /// Stop the child and everything it spawned.
    ///
    /// Asks politely first, waits up to `grace`, then kills. Returns the exit
    /// status if one was observed; `None` means the process was already reaped
    /// or never started.
    pub async fn terminate(&mut self, grace: Duration) -> io::Result<Option<ExitStatus>> {
        if self.child.id().is_none() {
            // Already exited and reaped.
            self.release_group();
            return Ok(None);
        }

        #[cfg(unix)]
        unix_impl::signal_group(&self.child, unix_impl::SIGTERM);
        #[cfg(windows)]
        // Windows has no graceful cross-process stop we can rely on for a
        // non-console child, so the job is terminated directly.
        self.job.terminate();

        match tokio::time::timeout(grace, self.child.wait()).await {
            Ok(status) => return status.map(Some),
            Err(_) => {
                #[cfg(unix)]
                unix_impl::signal_group(&self.child, unix_impl::SIGKILL);
            }
        }

        // After SIGKILL (or a job terminate) the wait cannot block for long.
        let status = self.child.wait().await.map(Some);
        self.release_group();
        status
    }

    /// Drop this group from the registry: it is gone, and a recycled pid
    /// must never be signalled in its name.
    fn release_group(&mut self) {
        if let Some(g) = self.group.take() {
            forget_group(g);
        }
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        self.release_group();
    }
}

#[cfg(unix)]
mod unix_impl {
    use std::os::unix::process::CommandExt;
    use tokio::process::{Child, Command};

    pub const SIGTERM: i32 = 15;
    pub const SIGKILL: i32 = 9;

    // Declared directly rather than pulling in `libc` for one call: the
    // signature is fixed by POSIX and this crate has no other need for it.
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }

    /// Put the child in a new process group of its own, with the child as
    /// group leader, so its descendants are reachable as one group.
    pub fn prepare(cmd: &mut Command) {
        cmd.as_std_mut().process_group(0);
    }

    /// Signal the child's whole process group.
    ///
    /// Best-effort: the only failure that matters (`ESRCH`) means the group is
    /// already gone, which is the outcome we wanted.
    pub fn signal_group(child: &Child, sig: i32) {
        let Some(pid) = child.id() else { return };
        let Ok(pid) = i32::try_from(pid) else { return };
        signal_pgid(pid, sig);
    }

    /// Signal a process group by the pid of its leader.
    pub fn signal_pgid(pgid: i32, sig: i32) {
        // SAFETY: `kill` has no memory effects; a stale pid only yields ESRCH.
        // The negative pid targets the group led by `pgid`.
        unsafe {
            kill(-pgid, sig);
        }
    }
}

#[cfg(windows)]
mod windows_impl {
    use std::io;
    use tokio::process::{Child, Command};
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject, TerminateJobObject,
    };
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW};

    /// Owns a Job Object handle. Closing the last handle to a job configured
    /// with `KILL_ON_JOB_CLOSE` terminates everything still inside it, so
    /// dropping this is itself a cleanup.
    #[derive(Debug)]
    pub struct JobObject(HANDLE);

    // SAFETY: a job handle is an opaque kernel handle, valid in any thread of
    // the process, and every use below goes through the Win32 API which does
    // its own synchronization.
    unsafe impl Send for JobObject {}
    unsafe impl Sync for JobObject {}

    impl JobObject {
        pub fn create() -> io::Result<Self> {
            // SAFETY: both arguments are documented as optional and null is
            // the documented way to request an unnamed job with default
            // security.
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            let job = JobObject(handle);

            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: `info` is a correctly sized, fully initialized struct of
            // the class named by `JobObjectExtendedLimitInformation`.
            let ok = unsafe {
                SetInformationJobObject(
                    job.0,
                    JobObjectExtendedLimitInformation,
                    (&raw const info).cast(),
                    u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                        .unwrap_or(u32::MAX),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(job)
        }

        /// Put an already-spawned child into the job.
        ///
        /// There is a window between spawn and assignment in which a
        /// grandchild could escape. It is not closable through
        /// `std::process::Command`, which offers no way to resume a process
        /// created suspended — and in practice yt-dlp performs network I/O
        /// long before it spawns ffmpeg, so the window is never live.
        pub fn assign(&self, child: &Child) -> io::Result<()> {
            let Some(handle) = child.raw_handle() else {
                // Already exited; nothing to contain.
                return Ok(());
            };
            // SAFETY: `handle` is the live process handle owned by `child`,
            // which outlives this call.
            let ok = unsafe { AssignProcessToJobObject(self.0, handle as HANDLE) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        /// Raw handle, for the registry that outlives this value's scope.
        pub fn raw(&self) -> usize {
            self.0 as usize
        }

        /// Kill every process in the job.
        pub fn terminate(&self) {
            // SAFETY: `self.0` is a valid job handle for this object's
            // lifetime. Failure here means the job is already gone.
            unsafe {
                TerminateJobObject(self.0, 1);
            }
        }
    }

    impl Drop for JobObject {
        fn drop(&mut self) {
            // SAFETY: handle is owned by this struct and closed exactly once.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    /// Kill a job named by a raw handle. Used by the registry, which holds
    /// handles rather than the owning values.
    pub fn terminate_job(handle: usize) {
        // SAFETY: the registry only ever holds handles of jobs that have not
        // been closed; terminating an already-dead job is a no-op failure.
        unsafe {
            TerminateJobObject(handle as HANDLE, 1);
        }
    }

    /// `CREATE_NEW_PROCESS_GROUP` keeps a console Ctrl+C from reaching the
    /// helper, so teardown happens on odl's terms through the job.
    /// `CREATE_NO_WINDOW` avoids a console flashing up for GUI consumers.
    pub fn prepare(cmd: &mut Command) {
        cmd.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Spawn a shell that starts a long-lived grandchild, then confirm the
    /// grandchild dies with the group rather than outliving it. This is the
    /// behaviour the whole module exists for.
    #[cfg(unix)]
    #[tokio::test]
    async fn terminate_kills_grandchildren() {
        let marker = tempfile::NamedTempFile::new().unwrap();
        let marker_path = marker.path().to_path_buf();

        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg(format!(
            // The grandchild outlives its parent shell and would keep writing
            // if only the direct child were killed.
            "( while true; do echo tick >> {p}; sleep 0.05; done ) & echo $! ; wait",
            p = marker_path.display()
        ));
        cmd.stdout(std::process::Stdio::piped());

        let mut managed = ManagedChild::spawn(&mut cmd).expect("spawn");
        // Let the grandchild write at least once.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let before = std::fs::metadata(&marker_path).unwrap().len();
        assert!(before > 0, "grandchild should have written something");

        managed
            .terminate(Duration::from_millis(500))
            .await
            .expect("terminate");

        // Give any survivor a chance to prove it is still running.
        let after_terminate = std::fs::metadata(&marker_path).unwrap().len();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let later = std::fs::metadata(&marker_path).unwrap().len();
        assert_eq!(
            after_terminate, later,
            "grandchild kept writing after terminate; the process group was not signalled"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn wait_reports_exit_status() {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("exit 3");
        let mut managed = ManagedChild::spawn(&mut cmd).expect("spawn");
        let status = managed.wait().await.expect("wait");
        assert_eq!(status.code(), Some(3));
    }

    /// The escape hatch must not become a leak: a hard exit still has to take
    /// the helpers with it.
    #[cfg(unix)]
    #[tokio::test]
    async fn killing_all_groups_reaches_grandchildren() {
        let marker = tempfile::NamedTempFile::new().unwrap();
        let path = marker.path().to_path_buf();

        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg(format!(
            "( while true; do echo tick >> {p}; sleep 0.05; done ) & wait",
            p = path.display()
        ));
        let managed = ManagedChild::spawn(&mut cmd).expect("spawn");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(std::fs::metadata(&path).unwrap().len() > 0);

        // Deliberately not `terminate`: this is the path taken when the
        // process is about to exit and no destructor will run.
        kill_all_groups();

        let after = std::fs::metadata(&path).unwrap().len();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            after,
            std::fs::metadata(&path).unwrap().len(),
            "a grandchild survived the hard-exit path"
        );
        drop(managed);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_finished_child_leaves_no_group_behind() {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("exit 0");
        let mut managed = ManagedChild::spawn(&mut cmd).expect("spawn");
        let group = managed.group.expect("a spawned child has a group");
        assert!(LIVE_GROUPS.lock().unwrap().contains(&group));

        managed.wait().await.expect("wait");

        // Scoped to this child: the registry is process-wide and other tests
        // run alongside. A pid gets recycled, so signalling it later in the
        // name of a download that already ended would hit an unrelated
        // process.
        assert!(
            !LIVE_GROUPS.lock().unwrap().contains(&group),
            "a reaped child must not stay registered"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminate_on_already_exited_child_is_ok() {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("exit 0");
        let mut managed = ManagedChild::spawn(&mut cmd).expect("spawn");
        managed.wait().await.expect("wait");
        // Reaped already: terminate must not error or hang.
        let status = managed.terminate(Duration::from_millis(200)).await.unwrap();
        assert!(status.is_none());
    }
}
