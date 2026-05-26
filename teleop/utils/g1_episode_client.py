import getpass
import json
import os
import queue
import re
import shutil
import subprocess
import threading
import time

import zmq
import logging_mp

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

logger_mp = logging_mp.getLogger(__name__)

_RSYNC_PROGRESS_RE = re.compile(
    r"^\s*(?P<amount>[\d.,]+[A-Za-z]*)\s+(?P<pct>\d{1,3})%\s+"
    r"(?P<speed>\S+)\s+(?P<eta>\S+)"
)


class G1EpisodeClient:
    """PC-side controller for G1-local episode recording and low-priority pullback."""

    def __init__(
        self,
        host,
        task_dir,
        task_goal=None,
        task_desc=None,
        task_steps=None,
        frequency=30,
        recorder_port=60010,
        transfer_ip=None,
        transfer_user=None,
        transfer_ssh_port=22,
        transfer_bwlimit_kbps=4000,
        max_pending_add_items=0,
    ):
        self.host = host
        self.port = int(recorder_port)
        self.transfer_ip = transfer_ip or host
        self.transfer_user = transfer_user or getpass.getuser()
        self.transfer_ssh_port = int(transfer_ssh_port)
        self.transfer_bwlimit_kbps = int(transfer_bwlimit_kbps)
        self.max_pending_add_items = max(0, int(max_pending_add_items))
        self.task_dir = task_dir
        self.task_goal = task_goal
        self.task_desc = task_desc
        self.task_steps = task_steps
        self.frequency = frequency

        os.makedirs(self.task_dir, exist_ok=True)
        self._episode_id = self._next_episode_id()
        self._active_episode_id = None
        self._ready = True
        self._closed = False
        self._queue = queue.Queue()
        self._transfer_threads = []
        self._pending_transfers = []
        self._transfer_errors = []
        self._transfer_errors_lock = threading.Lock()
        self._transfer_lock = threading.Lock()
        self._pending_transfers_lock = threading.Lock()
        self._claimed_transfer_ids = set()
        self._dropped_add_items_since_log = 0
        self._last_drop_log_t = 0.0
        self._worker = threading.Thread(target=self._run, daemon=True)
        self._worker.start()
        logger_mp.info(
            "[G1EpisodeClient] recording on G1 %s:%d, deferred transfer via %s to %s, max_pending_add_items=%d",
            self.host,
            self.port,
            self.transfer_ip,
            self.task_dir,
            self.max_pending_add_items,
        )

    def _next_episode_id(self):
        episode_ids = []
        for name in os.listdir(self.task_dir):
            if not name.startswith("episode_"):
                continue
            suffix = name[len("episode_"):]
            if not suffix.isdigit():
                continue
            if os.path.isdir(os.path.join(self.task_dir, name)):
                episode_ids.append(int(suffix))
        if not episode_ids:
            return 0
        return max(episode_ids) + 1

    def _send_command(self, payload, host=None, timeout_ms=5000):
        ctx = zmq.Context.instance()
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(f"tcp://{host or self.host}:{self.port}")
        try:
            sock.send_json(payload)
            poller = zmq.Poller()
            poller.register(sock, zmq.POLLIN)
            events = dict(poller.poll(timeout_ms))
            if sock not in events:
                raise TimeoutError(f"G1 recorder command timeout: {payload.get('cmd')}")
            reply = sock.recv_json()
            if not reply.get("ok", False):
                raise RuntimeError(reply.get("error", f"G1 recorder command failed: {payload.get('cmd')}"))
            return reply
        finally:
            sock.close()

    def _send_command_with_retries(self, payload, attempts=1, timeout_ms=5000, retry_sleep_s=0.5):
        last_exc = None
        for attempt in range(max(1, int(attempts))):
            try:
                return self._send_command(payload, timeout_ms=timeout_ms)
            except Exception as exc:
                last_exc = exc
                if attempt + 1 < attempts:
                    logger_mp.warning(
                        "[G1EpisodeClient] command %s attempt %d/%d failed: %s; retrying...",
                        payload.get("cmd"),
                        attempt + 1,
                        attempts,
                        exc,
                    )
                    time.sleep(retry_sleep_s)
        raise last_exc

    def _open_command_socket(self):
        ctx = zmq.Context.instance()
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(f"tcp://{self.host}:{self.port}")
        return sock

    def _send_command_on_socket(self, sock, payload, timeout_ms=5000):
        sock.send_json(payload)
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        events = dict(poller.poll(timeout_ms))
        if sock not in events:
            raise TimeoutError(f"G1 recorder command timeout: {payload.get('cmd')}")
        reply = sock.recv_json()
        if not reply.get("ok", False):
            raise RuntimeError(reply.get("error", f"G1 recorder command failed: {payload.get('cmd')}"))
        return reply

    def _run(self):
        sock = None
        try:
            sock = self._open_command_socket()
        except Exception as exc:
            logger_mp.warning("[G1EpisodeClient] failed to open persistent command socket: %s", exc)
        while True:
            item = self._queue.get()
            if item is None:
                self._queue.task_done()
                break
            cmd, payload, done, result = item
            try:
                attempts = 5 if cmd == "stop_episode" else 2 if cmd == "start_episode" else 1
                timeout_ms = 15000 if cmd == "stop_episode" else 8000 if cmd == "start_episode" else 5000
                last_exc = None
                for attempt in range(attempts):
                    try:
                        if sock is None:
                            sock = self._open_command_socket()
                        reply = self._send_command_on_socket(sock, payload, timeout_ms=timeout_ms)
                        break
                    except Exception as exc:
                        last_exc = exc
                        try:
                            if sock is not None:
                                sock.close()
                        except Exception:
                            pass
                        sock = None
                        if attempt + 1 < attempts:
                            logger_mp.warning(
                                "[G1EpisodeClient] command %s attempt %d/%d failed: %s; retrying...",
                                cmd,
                                attempt + 1,
                                attempts,
                                exc,
                            )
                            time.sleep(0.5)
                else:
                    raise last_exc
                if cmd == "stop_episode":
                    self._queue_transfer(reply)
                    self._ready = True
                if result is not None:
                    result["reply"] = reply
                    result["ok"] = True
            except Exception as exc:
                logger_mp.error("[G1EpisodeClient] command %s failed: %s", cmd, exc)
                if cmd == "stop_episode":
                    self._ready = True
                if result is not None:
                    result["ok"] = False
                    result["error"] = str(exc)
            finally:
                if done is not None:
                    done.set()
                self._queue.task_done()
        try:
            if sock is not None:
                sock.close()
        except Exception:
            pass

    def _enqueue(self, cmd, payload, wait=False):
        done = threading.Event() if wait else None
        result = {} if wait else None
        self._queue.put((cmd, payload, done, result))
        if wait:
            done.wait()
            if not result.get("ok", False):
                raise RuntimeError(result.get("error", f"{cmd} failed"))
            return result.get("reply", {})
        return None

    def _drop_pending_add_items(self, keep_latest=0, reason=""):
        """Drop stale queued frames that have not yet reached the G1 recorder."""
        if self.max_pending_add_items <= 0:
            return 0
        keep_latest = max(0, int(keep_latest))
        dropped = 0
        with self._queue.mutex:
            items = list(self._queue.queue)
            add_item_indices = [
                idx
                for idx, item in enumerate(items)
                if item is not None and item[0] == "add_item"
            ]
            drop_count = max(0, len(add_item_indices) - keep_latest)
            if drop_count <= 0:
                return 0

            drop_indices = set(add_item_indices[:drop_count])
            self._queue.queue.clear()
            for idx, item in enumerate(items):
                if idx in drop_indices:
                    dropped += 1
                    continue
                self._queue.queue.append(item)
            self._queue.unfinished_tasks = max(0, self._queue.unfinished_tasks - dropped)
            self._queue.all_tasks_done.notify_all()

        if dropped:
            if reason == "stop":
                logger_mp.warning(
                    "[G1EpisodeClient] dropped %d stale queued frame(s) before stop_episode.",
                    dropped,
                )
            else:
                self._dropped_add_items_since_log += dropped
                now = time.monotonic()
                if now - self._last_drop_log_t >= 2.0:
                    logger_mp.warning(
                        "[G1EpisodeClient] dropped %d stale queued frame(s); "
                        "keeping at most %d pending frame(s) for alignment.",
                        self._dropped_add_items_since_log,
                        self.max_pending_add_items,
                    )
                    self._dropped_add_items_since_log = 0
                    self._last_drop_log_t = now
        return dropped

    def _pending_add_item_count(self):
        with self._queue.mutex:
            return sum(1 for item in self._queue.queue if item is not None and item[0] == "add_item")

    def _episode_transfer_args(self, stop_reply):
        episode_id = stop_reply.get("episode_id")
        remote_path = stop_reply.get("episode_path")
        if remote_path is None or episode_id is None:
            logger_mp.error("[G1EpisodeClient] stop reply missing episode info: %s", stop_reply)
            return None
        local_path = os.path.join(self.task_dir, f"episode_{int(episode_id):04d}")
        return int(episode_id), remote_path, local_path

    def _queue_transfer(self, stop_reply):
        transfer_args = self._episode_transfer_args(stop_reply)
        if transfer_args is None:
            return
        episode_id = transfer_args[0]
        with self._pending_transfers_lock:
            if episode_id in self._claimed_transfer_ids:
                logger_mp.debug(
                    "[G1EpisodeClient] episode_%04d transfer already queued or running.",
                    episode_id,
                )
                return
            self._claimed_transfer_ids.add(episode_id)
            self._pending_transfers.append(transfer_args)
        logger_mp.info(
            "[G1EpisodeClient] episode_%04d transfer queued; press p to pull now or exit to pull later.",
            episode_id,
        )

    def _queue_remote_remaining_transfers(self):
        """Ask the G1 recorder for any stopped episodes the PC-side queue missed."""
        try:
            reply = self._send_command({"cmd": "list_episodes"}, timeout_ms=5000)
        except Exception as exc:
            logger_mp.warning(
                "[G1EpisodeClient] could not query G1 recorder for remaining episodes: %s",
                exc,
            )
            return 0

        added = 0
        for episode in reply.get("episodes", []):
            try:
                episode_id = int(episode["episode_id"])
                remote_path = episode["episode_path"]
            except Exception:
                logger_mp.warning("[G1EpisodeClient] invalid remote episode entry: %s", episode)
                continue
            if episode.get("active", False):
                continue
            local_path = os.path.join(self.task_dir, f"episode_{episode_id:04d}")
            if os.path.isdir(local_path):
                try:
                    self._verify_local_episode(local_path)
                    logger_mp.warning(
                        "[G1EpisodeClient] remote episode_%04d also exists locally; skipped remote recovery to avoid overwrite.",
                        episode_id,
                    )
                    continue
                except Exception:
                    pass
            with self._pending_transfers_lock:
                if episode_id in self._claimed_transfer_ids:
                    continue
                self._claimed_transfer_ids.add(episode_id)
                self._pending_transfers.append((episode_id, remote_path, local_path))
                added += 1
        if added:
            logger_mp.warning(
                "[G1EpisodeClient] recovered %d G1 episode transfer(s) from remote recorder state.",
                added,
            )
        return added

    def _start_transfer(self, episode_id, remote_path, local_path):
        t = threading.Thread(
            target=self._transfer_episode,
            args=(episode_id, remote_path, local_path),
            daemon=True,
        )
        t.start()
        self._transfer_threads.append(t)

    def _start_pending_transfers(self):
        with self._pending_transfers_lock:
            pending = list(self._pending_transfers)
            self._pending_transfers.clear()
        for transfer_args in pending:
            self._start_transfer(*transfer_args)
        return len(pending)

    def transfer_pending(self):
        """Start transfer for all completed episodes that have not been pulled yet."""
        count = self._start_pending_transfers()
        if count:
            logger_mp.info("[G1EpisodeClient] started %d pending episode transfer(s).", count)
        else:
            logger_mp.info("[G1EpisodeClient] no pending completed episodes to transfer.")
        return count

    def is_transferring(self):
        return any(t.is_alive() for t in list(self._transfer_threads))

    def pending_transfer_count(self):
        with self._pending_transfers_lock:
            return len(self._pending_transfers)

    def _verify_local_episode(self, local_path):
        json_path = os.path.join(local_path, "data.json")
        if not os.path.isfile(json_path):
            raise RuntimeError(f"missing data.json in {local_path}")
        with open(json_path, "r", encoding="utf-8") as f:
            episode = json.load(f)
        data = episode.get("data")
        if not isinstance(data, list):
            raise RuntimeError(f"invalid data.json: data is not a list in {local_path}")
        for row in data:
            for section in ("colors", "depths", "audios"):
                entries = row.get(section) or {}
                if not isinstance(entries, dict):
                    raise RuntimeError(f"invalid {section} entry in {local_path}")
                for rel_path in entries.values():
                    if not rel_path:
                        continue
                    file_path = os.path.join(local_path, rel_path)
                    if not os.path.isfile(file_path) or os.path.getsize(file_path) <= 0:
                        raise RuntimeError(f"missing or empty referenced file: {file_path}")
        return True

    def _run_rsync_with_tqdm(self, rsync_cmd, episode_id):
        progress = None
        last_pct = 0
        last_log_pct = -5
        output_tail = []

        def handle_output(text):
            nonlocal last_pct, last_log_pct
            text = text.strip()
            if not text:
                return
            match = _RSYNC_PROGRESS_RE.search(text)
            if match is None:
                output_tail.append(text)
                del output_tail[:-20]
                logger_mp.debug("[G1EpisodeClient][rsync] %s", text)
                return

            pct = max(0, min(100, int(match.group("pct"))))
            amount = match.group("amount")
            speed = match.group("speed")
            eta = match.group("eta")
            if progress is not None:
                if pct >= last_pct:
                    progress.update(pct - last_pct)
                else:
                    progress.n = pct
                    progress.refresh()
                progress.set_postfix_str(f"{amount} {speed} eta {eta}", refresh=True)
            elif pct >= last_log_pct + 5 or pct == 100:
                logger_mp.info(
                    "[G1EpisodeClient] episode_%04d transfer %d%% (%s, %s, eta %s)",
                    episode_id,
                    pct,
                    amount,
                    speed,
                    eta,
                )
                last_log_pct = pct
            last_pct = pct

        proc = None
        try:
            if tqdm is not None:
                progress = tqdm(
                    total=100,
                    desc=f"episode_{episode_id:04d}",
                    unit="%",
                    dynamic_ncols=True,
                    leave=True,
                )
            else:
                logger_mp.warning("[G1EpisodeClient] tqdm is not installed; using compact transfer logs.")

            proc = subprocess.Popen(
                rsync_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            buf = []
            while True:
                ch = proc.stdout.read(1)
                if ch == "" and proc.poll() is not None:
                    break
                if not ch:
                    time.sleep(0.02)
                    continue
                if ch in ("\r", "\n"):
                    handle_output("".join(buf))
                    buf = []
                else:
                    buf.append(ch)
            handle_output("".join(buf))
            returncode = proc.wait()
            if returncode != 0:
                if output_tail:
                    logger_mp.error(
                        "[G1EpisodeClient] rsync failed; recent output: %s",
                        " | ".join(output_tail[-5:]),
                    )
                raise subprocess.CalledProcessError(returncode, rsync_cmd)
            if progress is not None and last_pct < 100:
                progress.update(100 - last_pct)
                progress.set_postfix_str("done", refresh=True)
        except Exception:
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
            raise
        finally:
            if progress is not None:
                progress.close()

    def _transfer_episode(self, episode_id, remote_path, local_path):
        try:
            logger_mp.info("[G1EpisodeClient] waiting for G1 episode_%04d to finalize...", episode_id)
            while True:
                status = self._send_command({"cmd": "status", "episode_id": episode_id}, timeout_ms=2000)
                if status.get("missing", False):
                    raise RuntimeError(f"remote episode_{episode_id:04d} is missing before transfer")
                if status.get("finalized", False):
                    break
                time.sleep(0.2)

            tmp_local = f"{local_path}.partial"
            if os.path.exists(tmp_local):
                shutil.rmtree(tmp_local)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            remote = f"{self.transfer_user}@{self.transfer_ip}:{remote_path.rstrip('/')}/"
            ssh_cmd = f"ssh -p {self.transfer_ssh_port}"
            rsync_cmd = [
                "nice", "-n", "10",
                "ionice", "-c2", "-n7",
                "rsync", "-a", "--delete", "--partial", "--human-readable", "--info=progress2",
                "--rsync-path=ionice -c2 -n7 nice -n 10 rsync",
                "-e", ssh_cmd,
            ]
            if self.transfer_bwlimit_kbps > 0:
                rsync_cmd.append(f"--bwlimit={self.transfer_bwlimit_kbps}")
            rsync_cmd.extend([remote, f"{tmp_local}/"])
            logger_mp.info("[G1EpisodeClient] pulling episode_%04d from %s", episode_id, remote)
            with self._transfer_lock:
                self._run_rsync_with_tqdm(rsync_cmd, episode_id)
                if os.path.exists(local_path):
                    shutil.rmtree(local_path)
                os.replace(tmp_local, local_path)
                self._verify_local_episode(local_path)
                self._send_command({"cmd": "delete_episode", "episode_id": episode_id}, timeout_ms=5000)
            logger_mp.info("[G1EpisodeClient] episode_%04d transferred and remote tmp cleared.", episode_id)
        except Exception as exc:
            logger_mp.error("[G1EpisodeClient] episode_%04d transfer failed: %s", episode_id, exc)
            with self._transfer_errors_lock:
                self._transfer_errors.append(f"episode_{episode_id:04d}: {exc}")

    def is_ready(self):
        return self._ready and not self.is_transferring()

    def create_episode(self):
        episode_id = self._episode_id
        self._episode_id += 1
        self._active_episode_id = episode_id
        self._ready = False
        payload = {
            "cmd": "start_episode",
            "episode_id": episode_id,
            "task_goal": self.task_goal,
            "task_desc": self.task_desc,
            "task_steps": self.task_steps,
            "frequency": self.frequency,
        }
        try:
            self._enqueue("start_episode", payload, wait=True)
        except Exception:
            self._active_episode_id = None
            self._ready = True
            raise
        logger_mp.info("[G1EpisodeClient] G1 episode_%04d started.", episode_id)
        return True

    def add_item(self, colors=None, depths=None, states=None, actions=None, tactiles=None, audios=None, sim_state=None):
        if self._active_episode_id is None:
            return
        if self.max_pending_add_items > 0:
            self._drop_pending_add_items(keep_latest=self.max_pending_add_items - 1, reason="realtime")
        self._enqueue(
            "add_item",
            {
                "cmd": "add_item",
                "episode_id": self._active_episode_id,
                "states": states,
                "actions": actions,
                "tactiles": tactiles,
                "audios": audios,
                "sim_state": sim_state,
            },
            wait=False,
        )

    def save_episode(self):
        if self._active_episode_id is None:
            self._ready = True
            return
        episode_id = self._active_episode_id
        self._active_episode_id = None
        self._ready = False
        pending = self._pending_add_item_count()
        if pending:
            logger_mp.info(
                "[G1EpisodeClient] flushing %d queued frame(s) before stop_episode.",
                pending,
            )
        self._enqueue("stop_episode", {"cmd": "stop_episode", "episode_id": episode_id}, wait=False)
        logger_mp.info("[G1EpisodeClient] G1 episode_%04d stop queued.", episode_id)

    def has_active_episode(self):
        return self._active_episode_id is not None

    def close(self):
        if self._active_episode_id is not None:
            self.save_episode()
        logger_mp.info("[G1EpisodeClient] waiting for recorder commands to finish...")
        self._queue.join()
        self._queue_remote_remaining_transfers()
        self._queue.put(None)
        self._worker.join(timeout=5.0)
        transfer_count = self._start_pending_transfers()
        if transfer_count:
            logger_mp.info(
                "[G1EpisodeClient] shutdown transfer started for %d pending episode(s).",
                transfer_count,
            )
        else:
            logger_mp.info("[G1EpisodeClient] no pending episode transfers at shutdown.")
        logger_mp.info("[G1EpisodeClient] waiting for episode transfers to finish...")
        while True:
            alive = [t for t in list(self._transfer_threads) if t.is_alive()]
            if not alive:
                break
            logger_mp.info("[G1EpisodeClient] waiting for %d episode transfer(s) to finish...", len(alive))
            for t in alive:
                t.join(timeout=5.0)
        self._closed = True
        with self._transfer_errors_lock:
            transfer_errors = list(self._transfer_errors)
        if transfer_errors:
            raise RuntimeError(
                "Some G1 episode transfers failed; remote tmp_data was kept for safety: "
                + "; ".join(transfer_errors)
            )
