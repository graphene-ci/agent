// The agent's PTY half: an interactive shell on the machine, framed
// over the SAME session stream the container commands ride. A pty
// session is mortal by nature — it lives inside one stream's serve and
// dies with it; there is nothing to reconnect, the next shell is a new
// session.
package session

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/creack/pty"

	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
)

// ptyFrameCap bounds one output frame so a `cat` of a big file cannot
// starve the container commands sharing the stream.
const ptyFrameCap = 16 * 1024

// ptys is the shells of ONE serve: opened by server frames, buried
// together when the stream ends.
type ptys struct {
	mu sync.Mutex
	m  map[string]*ptyShell
}

type ptyShell struct {
	master *os.File
	cmd    *exec.Cmd
}

func newPtys() *ptys { return &ptys{m: map[string]*ptyShell{}} }

// handle executes one pty frame from the server. Output frames go to
// outbox from the shell's own reader goroutine; handle itself stays
// cheap so a slow container command never delays keystrokes.
func (p *ptys) handle(ctx context.Context, cmd *agentpb.SessionResponse, outbox chan<- *agentpb.SessionRequest) bool {
	switch body := cmd.GetBody().(type) {
	case *agentpb.SessionResponse_OpenPty:
		p.open(ctx, body.OpenPty, outbox)
	case *agentpb.SessionResponse_PtyInput:
		p.input(body.PtyInput)
	case *agentpb.SessionResponse_PtyResize:
		p.resize(body.PtyResize)
	case *agentpb.SessionResponse_ClosePty:
		p.close(body.ClosePty.GetPtyId())
	default:
		return false
	}
	return true
}

func (p *ptys) open(ctx context.Context, req *agentpb.OpenPty, outbox chan<- *agentpb.SessionRequest) {
	id := req.GetPtyId()
	p.mu.Lock()
	if _, live := p.m[id]; live {
		p.mu.Unlock()
		return // a second OpenPty with a live id is a no-op
	}
	p.mu.Unlock()

	//nolint:gosec // the whole point IS running the operator's shell
	cmd := exec.CommandContext(ctx, loginShell(), "-l")
	cmd.Env = append(os.Environ(), "TERM=xterm-256color")
	// Drop privileges: the agent runs as root, but the operator's shell
	// should not. GRAPHENE_AGENT_PTY_USER names the unprivileged user to
	// run it as; unset, the shell inherits the agent's uid and a loud
	// warning says so — a root shell on a shared machine is a standing
	// risk, not a default to leave silent.
	if err := dropToPtyUser(cmd); err != nil {
		send(ctx, outbox, ptyClosed(id, -1, err.Error()))
		return
	}
	master, err := pty.StartWithSize(cmd, &pty.Winsize{
		Cols: uint16(min(req.GetCols(), 65535)), //nolint:gosec // capped
		Rows: uint16(min(req.GetRows(), 65535)), //nolint:gosec // capped
	})
	if err != nil {
		send(ctx, outbox, ptyClosed(id, -1, "shell did not start: "+err.Error()))
		return
	}
	p.mu.Lock()
	p.m[id] = &ptyShell{master: master, cmd: cmd}
	p.mu.Unlock()

	go func() {
		buf := make([]byte, ptyFrameCap)
		for {
			n, err := master.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				send(ctx, outbox, &agentpb.SessionRequest{Body: &agentpb.SessionRequest_PtyOutput{
					PtyOutput: &agentpb.PtyOutput{PtyId: id, Data: data},
				}})
			}
			if err != nil {
				break
			}
		}
		exit := int32(-1)
		if err := cmd.Wait(); err == nil {
			exit = 0
		} else if ee, ok := err.(*exec.ExitError); ok {
			exit = int32(ee.ExitCode()) //nolint:gosec // exit codes are small
		}
		p.mu.Lock()
		delete(p.m, id)
		p.mu.Unlock()
		send(ctx, outbox, ptyClosed(id, exit, ""))
	}()
}

func (p *ptys) input(in *agentpb.PtyInput) {
	p.mu.Lock()
	sh := p.m[in.GetPtyId()]
	p.mu.Unlock()
	if sh != nil {
		_, _ = sh.master.Write(in.GetData())
	}
}

func (p *ptys) resize(rs *agentpb.PtyResize) {
	p.mu.Lock()
	sh := p.m[rs.GetPtyId()]
	p.mu.Unlock()
	if sh != nil {
		_ = pty.Setsize(sh.master, &pty.Winsize{
			Cols: uint16(min(rs.GetCols(), 65535)), //nolint:gosec // capped
			Rows: uint16(min(rs.GetRows(), 65535)), //nolint:gosec // capped
		})
	}
}

// close buries one shell: SIGHUP to the process group, master closed —
// the reader goroutine sees EOF and reports PtyClosed. Closing an
// unknown id is a no-op: either side may have buried it first.
func (p *ptys) close(id string) {
	p.mu.Lock()
	sh := p.m[id]
	p.mu.Unlock()
	if sh == nil {
		return
	}
	if sh.cmd.Process != nil {
		_ = sh.cmd.Process.Signal(syscall.SIGHUP)
	}
	_ = sh.master.Close()
}

// closeAll buries every shell of a dying stream.
func (p *ptys) closeAll() {
	p.mu.Lock()
	ids := make([]string, 0, len(p.m))
	for id := range p.m {
		ids = append(ids, id)
	}
	p.mu.Unlock()
	for _, id := range ids {
		p.close(id)
	}
}

// loginShell picks the shell a person would get logging in: $SHELL
// when the environment carries one, else the running user's login
// shell from /etc/passwd, else bash, else sh. Under systemd the
// environment carries nothing, and falling straight to /bin/sh lands
// an operator in dash — no readline, no history, broken multibyte
// input.
// ptyWarnedRoot ensures the root-shell warning is logged once per
// process, not on every OpenPty.
var ptyWarnedRoot sync.Once

// dropToPtyUser sets the shell to run as GRAPHENE_AGENT_PTY_USER, an
// unprivileged account. Unset, the shell keeps the agent's uid (root on
// a systemd machine) and a one-time warning names the risk. A named
// user that does not exist is a hard error — a misconfigured drop must
// fail loud, not silently fall back to root.
func dropToPtyUser(cmd *exec.Cmd) error {
	name := strings.TrimSpace(os.Getenv("GRAPHENE_AGENT_PTY_USER"))
	if name == "" {
		if os.Geteuid() == 0 {
			ptyWarnedRoot.Do(func() {
				slog.Warn("PTY shell runs as root: set GRAPHENE_AGENT_PTY_USER to an unprivileged account to drop privileges")
			})
		}
		return nil
	}
	u, err := user.Lookup(name)
	if err != nil {
		return fmt.Errorf("pty user %q: %w", name, err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return fmt.Errorf("pty user %q: bad uid %q", name, u.Uid)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return fmt.Errorf("pty user %q: bad gid %q", name, u.Gid)
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)} //nolint:gosec // uid/gid from the passwd lookup
	home := u.HomeDir
	if home == "" {
		home = "/"
	}
	cmd.Env = append(cmd.Env, "HOME="+home, "USER="+name, "LOGNAME="+name)
	cmd.Dir = home
	return nil
}

func loginShell() string {
	if sh := os.Getenv("SHELL"); sh != "" {
		return sh
	}
	if u, err := user.Current(); err == nil {
		if raw, err := os.ReadFile("/etc/passwd"); err == nil {
			for _, line := range strings.Split(string(raw), "\n") {
				fields := strings.Split(line, ":")
				if len(fields) == 7 && fields[0] == u.Username && fields[6] != "" && fields[6] != "/usr/sbin/nologin" && fields[6] != "/bin/false" {
					return fields[6]
				}
			}
		}
	}
	if _, err := exec.LookPath("bash"); err == nil {
		return "bash"
	}
	return "/bin/sh"
}

func ptyClosed(id string, exit int32, msg string) *agentpb.SessionRequest {
	return &agentpb.SessionRequest{Body: &agentpb.SessionRequest_PtyClosed{
		PtyClosed: &agentpb.PtyClosed{PtyId: id, ExitCode: exit, Message: msg},
	}}
}

func send(ctx context.Context, outbox chan<- *agentpb.SessionRequest, msg *agentpb.SessionRequest) {
	select {
	case outbox <- msg:
	case <-ctx.Done():
	}
}
