# Copyright 2025 YuShu TECHNOLOGY CO.,LTD ("Unitree Robotics")
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#cjx对源码进行了修改，uvc模式可能会出问题，如果要使用uvc模式，需要在reload_uvc_driver()中添加reload_uvc_driver()函数
import os
import argparse
import glob
import cv2
import numpy as np
import uvc
import yaml
import time
import threading
import signal
import functools
import subprocess
import platform
import struct
from .image_client import TripleRingBuffer, ZMQ_PublisherManager, ZMQ_Responser
# webrtc dependencies
import asyncio
import json
from aiohttp import web
from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from aiortc.rtcrtpsender import RTCRtpSender
from aiortc.contrib.media import MediaRelay
from aiortc.codecs import h264
import av
import ssl
from pathlib import Path
import queue
import fractions
from typing import Dict, List, Optional, Tuple, Any
import logging_mp
try:
    logging_mp.basic_config(level=logging_mp.INFO)
except RuntimeError:
    pass
logger_mp = logging_mp.get_logger(__name__)

IMAGE_PACKET_MAGIC = b"TELEIMG1\0"

def pack_image_packet(img_bytes: bytes, **metadata) -> bytes:
    metadata = {k: v for k, v in metadata.items() if v is not None}
    header = json.dumps(metadata, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return IMAGE_PACKET_MAGIC + struct.pack(">I", len(header)) + header + bytes(img_bytes)

def unpack_image_packet(payload: bytes):
    if not payload or not payload.startswith(IMAGE_PACKET_MAGIC):
        return {}, payload
    header_start = len(IMAGE_PACKET_MAGIC)
    header_end = header_start + 4
    if len(payload) < header_end:
        return {}, payload
    header_len = struct.unpack(">I", payload[header_start:header_end])[0]
    data_start = header_end + header_len
    if len(payload) < data_start:
        return {}, payload
    try:
        metadata = json.loads(payload[header_end:data_start].decode("utf-8"))
        if not isinstance(metadata, dict):
            metadata = {}
    except Exception:
        metadata = {}
    return metadata, payload[data_start:]

# ========================================================
# cam_config_server_from_G1.yaml path
# ========================================================
from pathlib import Path
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "cam_config_server_from_G1.yaml"
)
CONFIG_PATH = os.path.normpath(CONFIG_PATH)

# ========================================================
# certificate and key paths
# ========================================================
module_dir = Path(__file__).resolve().parent.parent.parent
default_cert = module_dir / "cert.pem"
default_key = module_dir / "key.pem"
env_cert = os.getenv("XR_TELEOP_CERT")
env_key = os.getenv("XR_TELEOP_KEY")
user_config_dir = Path.home() / ".config" / "xr_teleoperate"
user_cert = user_config_dir / "cert.pem"
user_key = user_config_dir / "key.pem"
CERT_PEM_PATH = Path(env_cert or (user_cert if user_cert.exists() else default_cert))
KEY_PEM_PATH = Path(env_key or (user_key if user_key.exists() else default_key))
CERT_PEM_PATH = CERT_PEM_PATH.resolve()
KEY_PEM_PATH = KEY_PEM_PATH.resolve()

# ========================================================
# libx264 for Jetson (Patch h264 Encoder)
# ========================================================
def jetson_software_encode_frame(self, frame: av.VideoFrame, force_keyframe: bool):
    if self.codec and (frame.width != self.codec.width or frame.height != self.codec.height):
        self.codec = None

    if self.codec is None:
        try:
            self.codec = av.CodecContext.create("libx264", "w")
            self.codec.width = frame.width
            self.codec.height = frame.height
            self.codec.bit_rate = self.target_bitrate
            self.codec.pix_fmt = "yuv420p"
            self.codec.framerate = fractions.Fraction(30, 1)
            self.codec.time_base = fractions.Fraction(1, 30)
        
            self.codec.options = {
                "preset": "ultrafast",
                "tune": "zerolatency",
                "threads": "1",
                "g": "60",
            }
            self.frame_count = 0
            force_keyframe = True
        except Exception as e:
            logger_mp.error(f"[H264 Patch] Initialization failed: {e}")
            return

    if not force_keyframe and hasattr(self, "frame_count") and self.frame_count % 60 == 0:
        force_keyframe = True
    
    self.frame_count = self.frame_count + 1 if hasattr(self, "frame_count") else 1
    frame.pict_type = av.video.frame.PictureType.I if force_keyframe else av.video.frame.PictureType.NONE

    try:
        for packet in self.codec.encode(frame):
            data = bytes(packet)
            if data:
                yield from self._split_bitstream(data)
    except Exception as e:
        logger_mp.warning(f"[H264 Patch] Encode error: {e}")

h264.H264Encoder._encode_frame = jetson_software_encode_frame

# ========================================================
# Embed HTML and JS directly
# ========================================================
INDEX_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>WebRTC Stream</title>
    <style>
    body { 
        font-family: sans-serif; 
        background: #fff; 
        color: #000; 
        text-align: center; 
    }
    button { padding: 10px 20px; font-size: 16px; cursor: pointer; }
    video { width: 100%; max-width: 1280px; background: #000; margin-top: 10px; }
    
    /* Title link style */
    h1 a {
        text-decoration: none;
        color: #000;
    }
    h1 a:hover {
        color: #555;
    }
    </style>
</head>
<body>
    <h1>
        <a href="https://github.com/unitreerobotics/teleimager" target="_blank">
            XR Teleoperation WebRTC Camera Stream
        </a>
    </h1>

    <div style="margin-bottom: 20px;">
        <a href="https://www.unitree.com/" target="_blank">
            <img src="https://www.unitree.com/images/0079f8938336436e955ea3a98c4e1e59.svg" alt="Unitree LOGO" width="10%">
        </a>
    </div>

    <button id="start" onclick="start()">Start</button>
    <button id="stop" style="display: none" onclick="stop()">Stop</button>
    
    <div id="media">
        <video id="video" autoplay playsinline muted></video>
        <audio id="audio" autoplay></audio>
    </div>
    
    <script src="client.js"></script>
</body>
</html>
"""

CLIENT_JS = """
var pc = null;

function negotiate() {
    pc.addTransceiver('video', { direction: 'recvonly' });
    return pc.createOffer().then((offer) => {
        return pc.setLocalDescription(offer);
    }).then(() => {
        return new Promise((resolve) => {
            if (pc.iceGatheringState === 'complete') {
                resolve();
            } else {
                const checkState = () => {
                    if (pc.iceGatheringState === 'complete') {
                        pc.removeEventListener('icegatheringstatechange', checkState);
                        resolve();
                    }
                };
                pc.addEventListener('icegatheringstatechange', checkState);
            }
        });
    }).then(() => {
        var offer = pc.localDescription;
        return fetch('/offer', {
            body: JSON.stringify({
                sdp: offer.sdp,
                type: offer.type,
            }),
            headers: {
                'Content-Type': 'application/json'
            },
            method: 'POST'
        });
    }).then((response) => {
        return response.json();
    }).then((answer) => {
        return pc.setRemoteDescription(answer);
    }).catch((e) => {
        alert(e);
    });
}

function start() {
    var config = {
        sdpSemantics: 'unified-plan'
    };

    // Removed STUN server check logic completely

    pc = new RTCPeerConnection(config);

    pc.addEventListener('track', (evt) => {
        if (evt.track.kind == 'video') {
            document.getElementById('video').srcObject = evt.streams[0];
        } else {
            document.getElementById('audio').srcObject = evt.streams[0];
        }
    });

    document.getElementById('start').style.display = 'none';
    negotiate();
    document.getElementById('stop').style.display = 'inline-block';
}

function stop() {
    document.getElementById('stop').style.display = 'none';
    document.getElementById('start').style.display = 'inline-block';
    if (pc) {
        pc.close();
        pc = null;
    }
}
"""

# ========================================================
# WebRTC publish
# ========================================================
class BGRArrayVideoStreamTrack(MediaStreamTrack):
    """MediaStreamTrack exposing BGR ndarrays as av.VideoFrame (latest-frame semantics)."""
    kind = "video"

    def __init__(self):
        super().__init__()
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._start_time = None
        self._pts = 0

    async def recv(self) -> av.VideoFrame:
        # This will suspend execution until a frame is available
        # preventing CPU busy-waiting
        frame = await self._queue.get()
        return frame

    def push_frame(self, bgr_numpy: np.ndarray, loop: Optional[asyncio.AbstractEventLoop] = None):
        if bgr_numpy is None:
            return

        # 1. Convert and calculate PTS immediately
        # MediaRelay requires consistent PTS to function correctly
        try:
            video_frame = av.VideoFrame.from_ndarray(bgr_numpy, format="bgr24")
            
            if self._start_time is None:
                self._start_time = time.time()
                self._pts = 0
            else:
                # 90000 is the standard RTP clock rate for video
                # This ensures smooth playback
                self._pts = int((time.time() - self._start_time) * 90000)
            
            video_frame.pts = self._pts
            video_frame.time_base = fractions.Fraction(1, 90000)
            
        except Exception as e:
            logger_mp.debug(f"Conversion failed: {e}")
            return

        # 2. Push to queue thread-safely
        target_loop = loop or asyncio.get_event_loop()
        if target_loop.is_closed():
            return
            
        def _put():
            try:
                # Drop old frame if queue is full (Low Latency strategy)
                if self._queue.full():
                    self._queue.get_nowait()
                self._queue.put_nowait(video_frame)
            except Exception:
                pass

        target_loop.call_soon_threadsafe(_put)


class WebRTC_PublisherThread(threading.Thread):
    """
    Runs aiohttp + aiortc in a separate THREAD (not Process).
    This enables shared memory and removes Pickling overhead.
    """
    def __init__(self, port: int, host: str = "0.0.0.0", codec_pref: str = None):
        super().__init__(daemon=True)
        self._host = host
        self._port = port
        self._codec_pref = codec_pref
        self._app = web.Application()
        self._runner: Optional[web.AppRunner] = None
        self._pcs = set()
        self._start_event = threading.Event()
        self._stop_event = threading.Event()
        self._frame_queue = queue.Queue(maxsize=1)

        self._bgr_track: Optional[BGRArrayVideoStreamTrack] = None
        self._relay: Optional[MediaRelay] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # register routes
        self._app.router.add_get("/", self._index)
        self._app.router.add_get("/client.js", self._javascript)
        self._app.router.add_post("/offer", self._offer)

        self._app.router.add_options("/", self._options)
        self._app.router.add_options("/client.js", self._options)
        self._app.router.add_options("/offer", self._options)

    async def _index(self, request: web.Request) -> web.Response:
        return web.Response(content_type="text/html", text=INDEX_HTML)
    
    async def _javascript(self, request: web.Request) -> web.Response:
        return web.Response(content_type="application/javascript", text=CLIENT_JS)

    async def _options(self, request):
        return web.Response(headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        })

    async def _offer(self, request: web.Request) -> web.Response:
        params = await request.json()
        offer = RTCSessionDescription(sdp=params["sdp"], type=params["type"])

        pc = RTCPeerConnection()
        self._pcs.add(pc)

        # CORE LOGIC: Use MediaRelay to subscribe
        # This ensures encoding happens only once globally
        if self._bgr_track and self._relay:
            try:
                relayed_track = self._relay.subscribe(self._bgr_track)
                transceiver = pc.addTransceiver(relayed_track, direction="sendonly")
                capabilities = RTCRtpSender.getCapabilities("video")
                pref = (self._codec_pref or "h264").lower()

                if pref == "h264":
                    h264_codecs = [c for c in capabilities.codecs if c.mimeType == "video/H264"]
                    if h264_codecs:
                        transceiver.setCodecPreferences(h264_codecs)
                        logger_mp.info(f"[WebRTC] Preferred H264 for port:{self._port}")
                    else:
                        logger_mp.warning(f"[WebRTC] H264 preferred but not found, using auto-negotiation for port:{self._port}")
                        
                elif pref == "vp8":
                    vp8_codecs = [c for c in capabilities.codecs if c.mimeType == "video/VP8"]
                    if vp8_codecs:
                        transceiver.setCodecPreferences(vp8_codecs)
                        logger_mp.info(f"[WebRTC] Preferred VP8 for port:{self._port}")
                    else:
                        logger_mp.warning(f"[WebRTC] VP8 preferred but not found, using auto-negotiation for port:{self._port}")
                
                else:
                    h264_codecs = [c for c in capabilities.codecs if c.mimeType == "video/H264"]
                    if h264_codecs:
                        transceiver.setCodecPreferences(h264_codecs)
                        logger_mp.info(f"[WebRTC] Preferred codec '{pref}' not found, falling back to H264 for port:{self._port}")
                    else:
                        logger_mp.warning(f"[WebRTC] Preferred codec '{pref}' not found, using auto-negotiation for port:{self._port}")
                    
            except Exception as e:
                logger_mp.error(f"Relay subscription failed: {e}")

        @pc.on("connectionstatechange")
        async def on_connectionstatechange():
            if pc.connectionState in ["failed", "closed"]:
                await self._cleanup_pc(pc)

        await pc.setRemoteDescription(offer)
        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)

        return web.Response(
            content_type="application/json",
            text=json.dumps({"sdp": pc.localDescription.sdp, "type": pc.localDescription.type}),
            headers={"Access-Control-Allow-Origin": "*"}
        )

    async def _cleanup_pc(self, pc):
        self._pcs.discard(pc)
        try:
            await pc.close()
        except: pass

    def wait_for_start(self, timeout=1.0):
        return self._start_event.wait(timeout=timeout)

    def run(self):
        # Create a new Event Loop for this thread
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
        async def _main():
            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            
            # Init Track and Relay inside the loop
            self._bgr_track = BGRArrayVideoStreamTrack()
            self._relay = MediaRelay()

            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_context.load_cert_chain(CERT_PEM_PATH, KEY_PEM_PATH)
            site = web.TCPSite(self._runner, self._host, self._port, ssl_context=ssl_context)
            await site.start()
            self._start_event.set()
            
            # Frame Pushing Loop
            while not self._stop_event.is_set():
                try:
                    # Non-blocking check for new frames
                    if not self._frame_queue.empty():
                        # Get frame (no pickling overhead in Threads!)
                        frame = self._frame_queue.get_nowait()
                        self._bgr_track.push_frame(frame, loop=self._loop)
                    
                    # CRITICAL: Yield control to asyncio loop to handle WebRTC packets
                    await asyncio.sleep(0.005)
                except Exception:
                    await asyncio.sleep(0.005)

        try:
            self._loop.run_until_complete(_main())
        except Exception as e:
            logger_mp.error(f"WebRTC Thread Error: {e}")
        finally:
            if self._loop: self._loop.close()

    def send(self, data: np.ndarray):
        """Send data to the processing thread."""
        # Simple drop-frame logic if queue is full
        if not self._frame_queue.full():
            self._frame_queue.put(data)
        else:
            try:
                self._frame_queue.get_nowait()
                self._frame_queue.put(data)
            except: pass

    def stop(self):
        self._stop_event.set()
        self.join(timeout=1.0)


# ========================================================
# WebRTC Manager
# ========================================================
class WebRTC_PublisherManager:
    """Manages WebRTC_PublisherThreads."""
    _instance: Optional["WebRTC_PublisherManager"] = None
    _publisher_threads: Dict[Tuple[str, int], WebRTC_PublisherThread] = {}
    _lock = threading.Lock()
    _running = True

    def __init__(self):
        pass

    @classmethod
    def get_instance(cls) -> "WebRTC_PublisherManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _create_publisher(self, port: int, host: str, codec_pref: str):
        t = WebRTC_PublisherThread(port, host, codec_pref)
        t.start()
        if not t.wait_for_start(timeout=5.0):
             raise ConnectionError("Publisher failed to start (Timeout)")
        return t

    def _get_publisher(self, port, host, codec_pref):
        key = (host, port)
        with self._lock:
            if key not in self._publisher_threads:
                self._publisher_threads[key] = self._create_publisher(port, host, codec_pref)
            return self._publisher_threads[key]

    def publish(self, data: Any, port: int, host: str = "0.0.0.0", codec_pref: str = None) -> None:
        if not self._running: return
        try:
            pub = self._get_publisher(port, host, codec_pref)
            pub.send(data)
        except Exception as e:
            logger_mp.error(f"Unexpected error in publish: {e}")
            pass

    def close(self) -> None:
        self._running = False
        with self._lock:
            for key, pub in list(self._publisher_threads.items()):
                try:
                    pub.stop()
                except Exception: pass
            self._publisher_threads.clear()

# ========================================================
# V4L2 helpers (RealSense RGB via OpenCV)
# ========================================================
def _v4l2_ctl(video_path: str, *args: str, timeout: float = 3.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["v4l2-ctl", "-d", video_path, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _v4l2_has_capture_formats(video_path: str) -> bool:
    try:
        result = _v4l2_ctl(video_path, "--list-formats-ext")
        if result.returncode != 0:
            return False
        out = result.stdout
        return "Type: Video Capture" in out and ("Size: Discrete" in out or "Size: Stepwise" in out)
    except Exception:
        return False


def _v4l2_has_color_camera_controls(video_path: str) -> bool:
    """True RGB UVC nodes expose saturation / white balance (see REALSENSE_RGB_OPENCV_TROUBLESHOOTING.md)."""
    try:
        result = _v4l2_ctl(video_path, "--list-ctrls")
        if result.returncode != 0:
            return False
        text = result.stdout.lower()
        return "saturation" in text and ("white_balance" in text or "white_balance_temperature" in text)
    except Exception:
        return False


def _v4l2_supports_yuyv_capture(video_path: str) -> bool:
    try:
        result = _v4l2_ctl(video_path, "--list-formats-ext")
        if result.returncode != 0:
            return False
        out = result.stdout.upper()
        return "'YUYV'" in out or "'YUY2'" in out
    except Exception:
        return False


def _v4l2_can_stream_one_frame(video_path: str, width: int = 640, height: int = 480) -> bool:
    try:
        result = subprocess.run(
            [
                "v4l2-ctl", "-d", video_path,
                f"--set-fmt-video=width={width},height={height},pixelformat=YUYV",
                "--stream-mmap=3", "--stream-count=1",
            ],
            capture_output=True,
            text=True,
            timeout=5.0,
        )
        if result.returncode != 0:
            return False
        combined = (result.stdout or "") + (result.stderr or "")
        return "VIDIOC_STREAMON returned -1" not in combined and "Input/output error" not in combined
    except Exception:
        return False


def _v4l2_device_busy_hint(video_path: str) -> str:
    return (
        f"V4L2 device {video_path} is busy or cannot stream. "
        "On Unitree G1/PC4, release the camera with:\n"
        "  /unitree/sbin/mscli stopservice video_hub_pc4\n"
        "  (stopping wlr-video-hub.service alone is not enough — videohub_pc4 is separate)\n"
        "Optional: sudo systemctl stop wlr-video-hub.service"
    )


# ========================================================
# camera finder and cameras
# ========================================================
class CameraFinder:
    """
    Discover connected cameras and their properties.
    vpath: /dev/videoX
    ppath: physical path in /sys/class/video4linux, e.g. /sys/devices/pci0000:00/0000:00:14.0/usb1/1-11/1-11.2/1-11.2:1.0
    uid: USB unique ID, e.g. "001:002"
    dev_info: extra info from uvc
    sn: serial number of the camera
    """
    def __init__(self, realsense_enable=False, verbose=False):
        self.verbose = verbose
        # uvc
        self.uvc_devices = uvc.device_list()
        self.uid_map = {dev["uid"]: dev for dev in self.uvc_devices}
        # all video devices
        self.video_paths = self._list_video_paths()
        # realsense
        if realsense_enable:
            self.rs_serial_numbers = self._list_realsense_serial_numbers()
            self.rs_video_paths = self._list_realsense_video_paths()
            self.rs_rgb_video_paths = [p for p in self.rs_video_paths if self._is_like_rgb(p)]
        else:
            self.rs_serial_numbers = []
            self.rs_video_paths = []
            self.rs_rgb_video_paths = []
        # rgb & uvc
        self.uvc_rgb_video_paths = self._list_uvc_rgb_video_paths()
        self.uvc_rgb_video_ids = [int(v.replace("/dev/video", "")) for v in self.uvc_rgb_video_paths]
        self.uvc_rgb_physical_paths = [self._get_ppath_from_vpath(v) for v in self.uvc_rgb_video_paths]
        self.uvc_rgb_uids = [self._get_uid_from_ppath(p) for p in self.uvc_rgb_physical_paths]
        self.uvc_rgb_dev_info = [self.uid_map.get(uid) for uid in self.uvc_rgb_uids]
        self.uvc_rgb_serial_numbers = [dev_info.get("serialNumber") if dev_info else None for dev_info in self.uvc_rgb_dev_info]
        # all uvc cameras
        self.uvc_rgb_cameras = {}
        for vpath, vid, ppath, uid, dev_info, sn in zip(
            self.uvc_rgb_video_paths,
            self.uvc_rgb_video_ids,
            self.uvc_rgb_physical_paths,
            self.uvc_rgb_uids,
            self.uvc_rgb_dev_info,
            self.uvc_rgb_serial_numbers,
        ):
            self.uvc_rgb_cameras[vpath] = {
                "video_id": vid,
                "physical_path": ppath,
                "uid": uid,
                "dev_info": dev_info,
                "serial_number": sn
            }
        if self.verbose:
            self.info()

    # utils
    def _list_video_paths(self):
        base = "/sys/class/video4linux/"
        if not os.path.exists(base):
            return []
        return [f"/dev/{x}" for x in sorted(os.listdir(base)) if x.startswith("video")]

    def _list_uvc_rgb_video_paths(self):
        return [p for p in self.video_paths if self._is_like_rgb(p) and p not in self.rs_video_paths]

    def _list_realsense_video_paths(self):
        def _read_text(path):
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    return f.read().strip()
            except Exception:
                return None

        def _parent_usb_device_sysdir(video_sysdir):
            d = os.path.realpath(os.path.join(video_sysdir, "device"))
            for _ in range(10):
                if d is None or d == "/" or not os.path.isdir(d):
                    break
                id_vendor = _read_text(os.path.join(d, "idVendor"))
                id_product = _read_text(os.path.join(d, "idProduct"))
                if id_vendor and id_product:
                    return d
                d_next = os.path.dirname(d)
                if d_next == d:
                    break
                d = d_next
            return None

        ports = []
        for devnode in sorted(glob.glob("/dev/video*")):
            sysdir = f"/sys/class/video4linux/{os.path.basename(devnode)}"
            name = _read_text(os.path.join(sysdir, "name"))
            usb_dir = _parent_usb_device_sysdir(sysdir)
            vendor_id = _read_text(os.path.join(usb_dir, "idVendor")) if usb_dir else None

            # Match RealSense by name and Intel vendor ID
            if name and "realsense" in name.lower() and (vendor_id or "").lower() in ("8086", "32902"):
                ports.append(devnode)

        return ports
    
    def get_realsense_module(self) -> object:
        try:
            import pyrealsense2 as rs
            return rs
        except ImportError:
            arch = platform.machine()
            system = platform.system()
            print(f"[RealSense] Platform: {system} / {arch}")

            if system == "Linux" and arch.startswith("aarch64"):
                # Jetson NX / arm64
                msg = (
                    "[RealSense] pyrealsense2 not installed. please build from source:\n"
                    "    cd ~\n"
                    "    git clone https://github.com/IntelRealSense/librealsense.git\n"
                    "    cd librealsense\n"
                    "    git checkout v2.50.0\n"
                    "    mkdir build && cd build\n"
                    "    cmake .. -DBUILD_PYTHON_BINDINGS=ON -DPYTHON_EXECUTABLE=$(which python3)\n"
                    "    make -j$(nproc)\n"
                    "    sudo make install\n"
                )
            else:
                # x86/x64
                msg = (
                    "[RealSense] pyrealsense2 not installed. You can try:\n"
                    "    pip install pyrealsense2\n"
                )
            raise RuntimeError(msg)

    def _list_realsense_serial_numbers(self):
        rs = self.get_realsense_module()
        ctx = rs.context()
        devices = ctx.query_devices()
        serials = []
        for dev in devices:
            try:
                serials.append(dev.get_info(rs.camera_info.serial_number))
            except Exception:
                continue
        return serials

    def _get_ppath_from_vpath(self, video_path):
        sysfs_path = f"/sys/class/video4linux/{os.path.basename(video_path)}/device"
        return os.path.realpath(sysfs_path)

    def _get_uid_from_ppath(self, physical_path):
        def read_file(path):
            return open(path).read().strip() if os.path.exists(path) else None

        busnum_file = os.path.join(physical_path, "busnum")
        devnum_file = os.path.join(physical_path, "devnum")

        if not (os.path.exists(busnum_file) and os.path.exists(devnum_file)):
            parent = os.path.dirname(physical_path)
            busnum_file = os.path.join(parent, "busnum")
            devnum_file = os.path.join(parent, "devnum")

        if os.path.exists(busnum_file) and os.path.exists(devnum_file):
            bus = read_file(busnum_file)
            dev = read_file(devnum_file)
            return f"{bus}:{dev}"
        return None

    def _is_like_rgb(self, video_path):
        """V4L2 metadata check — do not use OpenCV 3-channel read (depth/IR false positives)."""
        if video_path not in self.video_paths:
            return False
        return (
            _v4l2_has_capture_formats(video_path)
            and _v4l2_has_color_camera_controls(video_path)
            and _v4l2_supports_yuyv_capture(video_path)
        )

    def is_rgb_vpath(self, video_path: str) -> bool:
        return self._is_like_rgb(video_path)

    def find_realsense_rgb_vpath(self, serial_number: Optional[str] = None) -> Optional[str]:
        """Pick the RealSense visible-light RGB /dev/video* node (YUYV + color controls)."""
        candidates: List[str] = []
        for vpath in self.video_paths:
            if not self._is_like_rgb(vpath):
                continue
            if serial_number:
                sysdir = f"/sys/class/video4linux/{os.path.basename(vpath)}"
                serial_path = os.path.join(sysdir, "device", "../../../../serial")
                if not os.path.exists(serial_path):
                    serial_path = os.path.join(sysdir, "device", "serial")
                sn = None
                try:
                    with open(serial_path, "r", encoding="utf-8", errors="ignore") as f:
                        sn = f.read().strip()
                except Exception:
                    pass
                if sn and str(serial_number) != sn:
                    continue
            candidates.append(vpath)

        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]

        # Prefer a node that can stream when multiple match (e.g. duplicate sysfs entries).
        streamable = [p for p in candidates if _v4l2_can_stream_one_frame(p)]
        if streamable:
            return sorted(streamable, key=lambda p: int(p.replace("/dev/video", "")))[0]
        return sorted(candidates, key=lambda p: int(p.replace("/dev/video", "")))[0]

    def resolve_opencv_video_path(
        self,
        video_path: Optional[str],
        serial_number: Optional[str] = None,
        physical_path: Optional[str] = None,
    ) -> Optional[str]:
        if serial_number:
            vpath = self.get_vpath_by_sn(serial_number)
            if vpath:
                return vpath
        if physical_path:
            vpath = self.get_vpath_by_ppath(physical_path)
            if vpath:
                return vpath
        if video_path and self.is_rgb_vpath(video_path):
            return video_path
        alt = self.find_realsense_rgb_vpath(serial_number)
        if alt and alt != video_path:
            logger_mp.warning(
                "[CameraFinder] Configured %s is not a V4L2 RGB node; using %s instead.",
                video_path,
                alt,
            )
        return alt or video_path

    # --------------------------------------------------------
    # public api
    # --------------------------------------------------------
    def is_rs_serial_exist(self, serial_number):
        return str(serial_number) in self.rs_serial_numbers

    def is_vpath_exist(self, vpath):
        return vpath in self.video_paths
    
    def is_ppath_exist(self, physical_path):
        for cam in self.uvc_rgb_cameras.values():
            if cam.get("physical_path") == physical_path:
                return True
        return False
    
    def get_uid_by_sn(self, serial_number):
        matches = [
            cam for cam in self.uvc_rgb_cameras.values()
            if cam.get("serial_number") == str(serial_number)
        ]
        if not matches:
            return None
        if len(matches) > 1:
            raise ValueError(f"Multiple cameras found with serial number {serial_number}")
        return matches[0].get("uid")

    def get_uid_by_ppath(self, physical_path):
        for cam in self.uvc_rgb_cameras.values():
            if cam.get("physical_path") == physical_path:
                return cam.get("uid")
        return None
    
    def get_uid_by_vpath(self, video_path):
        cam = self.uvc_rgb_cameras.get(video_path)
        if cam:
            return cam.get("uid")
        if not self.is_vpath_exist(video_path):
            return None
        physical_path = self._get_ppath_from_vpath(video_path)
        return self._get_uid_from_ppath(physical_path)
    
    def get_vpath_by_sn(self, serial_number):
        matches = []
        for cam in self.uvc_rgb_cameras.values():
            if cam.get("serial_number") == str(serial_number):
                vpath = f"/dev/video{cam.get('video_id')}"
                matches.append(vpath)
        if not matches:
            return None
        if len(matches) > 1:
            raise ValueError(f"Multiple video devices found for serial number {serial_number}: {matches}. ")
        return matches[0]

    def get_vpath_by_ppath(self, physical_path):
        base = "/sys/class/video4linux/"
        matches = []
        for v in os.listdir(base):
            sys_path = os.path.realpath(os.path.join(base, v, "device"))
            if sys_path == physical_path:
                vpath = f"/dev/{v}"
                if self._is_like_rgb(vpath):
                    matches.append(vpath)
        if not matches:
            return None
        if len(matches) > 1:
            raise ValueError(f"Multiple video devices found for physical path {physical_path}: {matches}. ")
        return matches[0]
    

    def info(self):
        logger_mp.info("======================= Camera Discovery Start ==================================")
        logger_mp.info("Found video devices: %s", self.video_paths)
        logger_mp.info("Found RGB video devices: %s", self.uvc_rgb_video_paths)

        if self.rs_serial_numbers:
            logger_mp.info("----------------------- Realsense Cameras ----------------------------------")
            logger_mp.info(f"RealSense serial numbers: {self.rs_serial_numbers}")
            logger_mp.info(f"RealSense video paths: {self.rs_video_paths}")
            logger_mp.info(f"RealSense RGB-like video paths: {self.rs_rgb_video_paths}")

        for idx, (vpath, cam) in enumerate(self.uvc_rgb_cameras.items(), start=1):
            logger_mp.info("----------------------- OpenCV / UVC Camera %d -----------------------------", idx)
            logger_mp.info("video_path    : %s", vpath)
            logger_mp.info("video_id      : %s", cam.get("video_id"))
            logger_mp.info("serial_number : %s", cam.get("serial_number") or "unknown")
            logger_mp.info("physical_path : %s", cam.get("physical_path"))
            logger_mp.info("extra_info:")

            dev_info = cam.get("dev_info")
            uid = cam.get("uid")

            if dev_info:
                for k, v in dev_info.items():
                    logger_mp.info("    %s: %s", k, v)
                try:
                    cap = uvc.Capture(uid)
                    for fmt in cap.available_modes:
                        logger_mp.info("    format: %dx%d@%d %s", fmt.height, fmt.width, fmt.fps, fmt.format_name)
                    cap.close()
                    cap = None
                except Exception as e:
                    logger_mp.warning("    failed to get formats: %s", e)
            else:
                logger_mp.info("    no uvc extra info available")

        logger_mp.info("=========================== Camera Discovery End ================================")

class BaseCamera:
    def __init__(self, cam_topic, img_shape, fps, 
                 enable_zmq=True, zmq_port=55555, enable_webrtc=False, webrtc_port=66666, webrtc_codec=None):
        self._ready = threading.Event()
        self._cam_topic = cam_topic
        self._img_shape = img_shape # (H, W)
        self._fps = fps
        self._enable_zmq = enable_zmq
        self._zmq_port = zmq_port
        if self._enable_zmq:
            self._zmq_buffer = TripleRingBuffer()
        else:
            self._zmq_buffer = None

        self._enable_webrtc = enable_webrtc
        self._webrtc_port = webrtc_port
        self._webrtc_codec = webrtc_codec
        if self._enable_webrtc:
            self._webrtc_buffer = TripleRingBuffer()
        else:
            self._webrtc_buffer = None

    def __str__(self):
        raise NotImplementedError
    
    def __repr__(self):
        return self.__str__()

    def _update_frame(self):
        """Return a jepg frame as bytes, and a bgr frame as numpy array"""
        raise NotImplementedError
    
    def wait_until_ready(self, timeout=None):
        """Block until the camera is ready (first frame is available) or timeout occurs."""
        return self._ready.wait(timeout=timeout)

    def enable_webrtc(self):
        return self._enable_webrtc
    
    def enable_zmq(self):
        return self._enable_zmq

    def get_jpeg_bytes(self):
        jpeg_bytes = self._zmq_buffer.read() if self._enable_zmq and self._zmq_buffer else None
        return jpeg_bytes

    def get_bgr_frame(self):
        bgr_numpy = self._webrtc_buffer.read() if self._enable_webrtc and self._webrtc_buffer else None
        return bgr_numpy

    def get_depth_frame(self):
        """Return a depth frame as bytes, or None if not supported. 
           Before call this function, must first call get_frame() to update the latest depth data."""
        return None

    def get_zmq_port(self):
        """Return the zmq port number the camera is serving on."""
        return self._zmq_port
    
    def get_webrtc_port(self):
        """Return the webrtc port number the camera is serving on."""
        return self._webrtc_port
    
    def get_webrtc_codec(self):
        """Return the webrtc codec setting."""
        return self._webrtc_codec

    def get_fps(self):
        """Return the camera FPS setting."""
        return self._fps

    def release(self):
        """Release camera resources."""
        raise NotImplementedError

class SharedRealSenseRGBDSource:
    _instances = {}
    _registry_lock = threading.Lock()

    @classmethod
    def acquire(cls, rs, serial_number, img_shape, fps):
        key = (str(serial_number), int(img_shape[0]), int(img_shape[1]), int(fps))
        with cls._registry_lock:
            source = cls._instances.get(key)
            if source is None:
                source = cls(rs, serial_number, img_shape, fps)
                cls._instances[key] = source
            source._ref_count += 1
            return source

    def __init__(self, rs, serial_number, img_shape, fps):
        self._rs = rs
        self._serial_number = str(serial_number)
        self._img_shape = img_shape
        self._fps = fps
        self._ref_count = 0
        self._running = True
        self._ready = threading.Event()
        self._lock = threading.Lock()
        self._processing_lock = threading.Lock()
        self._color_bgr = None
        self._depth_z16 = None
        self._frame_number = None
        self._timestamp_ms = None
        self._depth_scale = 0.001
        self._depth_align_to_color = True
        self._depth_align_fill_holes = False
        self._depth_align_fill_iterations = 1
        self._depth_filters = []

        self.pipeline = rs.pipeline()
        config = rs.config()
        config.enable_device(self._serial_number)
        config.enable_stream(rs.stream.color, self._img_shape[1], self._img_shape[0], rs.format.bgr8, self._fps)
        config.enable_stream(rs.stream.depth, self._img_shape[1], self._img_shape[0], rs.format.z16, self._fps)
        profile = self.pipeline.start(config)
        self._device = profile.get_device()
        depth_sensor = self._device.first_depth_sensor()
        self._depth_scale = depth_sensor.get_depth_scale()
        self._align = rs.align(rs.stream.color)
        self.color_intrinsics = profile.get_stream(rs.stream.color).as_video_stream_profile().get_intrinsics()
        self.depth_intrinsics = profile.get_stream(rs.stream.depth).as_video_stream_profile().get_intrinsics()

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger_mp.info(
            f"[SharedRealSenseRGBDSource] started serial={self._serial_number} "
            f"{self._img_shape[0]}x{self._img_shape[1]} @ {self._fps} FPS."
        )

    @property
    def depth_scale(self):
        return self._depth_scale

    def _set_filter_option(self, filter_obj, option, value):
        try:
            if hasattr(filter_obj, "supports") and not filter_obj.supports(option):
                return
            filter_obj.set_option(option, value)
        except Exception:
            pass

    def configure_depth_processing(
        self,
        align_to_color=True,
        align_fill_holes=False,
        align_fill_iterations=1,
        spatial_filter=False,
        temporal_filter=False,
        hole_filling=False,
        hole_filling_mode=1,
    ):
        rs = self._rs
        filters = []
        if spatial_filter:
            spatial = rs.spatial_filter()
            self._set_filter_option(spatial, rs.option.filter_magnitude, 2)
            self._set_filter_option(spatial, rs.option.filter_smooth_alpha, 0.5)
            self._set_filter_option(spatial, rs.option.filter_smooth_delta, 20)
            self._set_filter_option(spatial, rs.option.holes_fill, 1)
            filters.append(spatial)
        if temporal_filter:
            filters.append(rs.temporal_filter())
        if hole_filling:
            hole_filling_filter = rs.hole_filling_filter()
            self._set_filter_option(hole_filling_filter, rs.option.holes_fill, int(hole_filling_mode))
            filters.append(hole_filling_filter)

        with self._processing_lock:
            self._depth_align_to_color = bool(align_to_color)
            self._depth_align_fill_holes = bool(align_fill_holes)
            self._depth_align_fill_iterations = max(0, int(align_fill_iterations))
            self._depth_filters = filters

    def _fill_depth_holes(self, depth_z16, iterations):
        if iterations <= 0:
            return depth_z16
        filled = depth_z16.copy()
        kernel = np.ones((3, 3), dtype=np.uint8)
        for _ in range(iterations):
            holes = filled == 0
            if not np.any(holes):
                break
            dilated = cv2.dilate(filled, kernel)
            filled[holes] = dilated[holes]
        return filled

    def _run(self):
        while self._running:
            try:
                frames = self.pipeline.wait_for_frames()
                with self._processing_lock:
                    align_to_color = self._depth_align_to_color
                    align_fill_holes = self._depth_align_fill_holes
                    align_fill_iterations = self._depth_align_fill_iterations
                    filters = list(self._depth_filters)

                if align_to_color:
                    frames = self._align.process(frames)

                color_frame = frames.get_color_frame()
                depth_frame = frames.get_depth_frame()
                if not color_frame or not depth_frame:
                    continue

                for depth_filter in filters:
                    depth_frame = depth_filter.process(depth_frame)

                color_bgr = np.asanyarray(color_frame.get_data())
                depth_z16 = np.asanyarray(depth_frame.get_data())
                if align_fill_holes:
                    depth_z16 = self._fill_depth_holes(depth_z16, align_fill_iterations)

                with self._lock:
                    self._color_bgr = color_bgr.copy()
                    self._depth_z16 = depth_z16.copy()
                    self._frame_number = color_frame.get_frame_number()
                    self._timestamp_ms = color_frame.get_timestamp()
                    self._ready.set()
            except Exception as e:
                if self._running:
                    logger_mp.error(f"[SharedRealSenseRGBDSource] update failed: {e}")
                    time.sleep(0.01)

    def latest_color(self):
        with self._lock:
            return None if self._color_bgr is None else self._color_bgr.copy()

    def latest_depth(self):
        with self._lock:
            return None if self._depth_z16 is None else self._depth_z16.copy()

    def wait_until_ready(self, timeout=5.0):
        return self._ready.wait(timeout=timeout)

    def release_ref(self):
        with self._registry_lock:
            self._ref_count -= 1
            if self._ref_count > 0:
                return
            key = (self._serial_number, int(self._img_shape[0]), int(self._img_shape[1]), int(self._fps))
            self._instances.pop(key, None)
        self.close()

    def close(self):
        self._running = False
        if hasattr(self, "_thread") and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        try:
            self.pipeline.stop()
        except Exception as e:
            logger_mp.warning(f"[SharedRealSenseRGBDSource] pipeline.stop() failed: {e}")
        logger_mp.info(f"[SharedRealSenseRGBDSource] stopped serial={self._serial_number}")


class RealSenseCamera(BaseCamera):
    def __init__(self, cam_topic, serial_number, img_shape, fps, 
                 enable_zmq=True, zmq_port = 55555, enable_webrtc=False, webrtc_port=66666, webrtc_codec=None, enable_depth=False,
                 stream="color", depth_visual_min_m=0.15, depth_visual_max_m=3.0,
                 depth_colormap="turbo", depth_invert=False, depth_align_to_color=False,
                 depth_align_fill_holes=False, depth_align_fill_iterations=1,
                 depth_spatial_filter=False, depth_temporal_filter=False,
                 depth_hole_filling=False, depth_hole_filling_mode=1,
                 shared_rgbd=False, depth_png_compression=1):
        rs = self.check_pyrealsense2_install()
        super().__init__(cam_topic, img_shape, fps, enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec)
        self._rs = rs
        self._serial_number = str(serial_number) if serial_number else None
        self._enable_depth = enable_depth
        self._stream = str(stream or "color").lower()
        if self._stream not in ("color", "depth"):
            raise ValueError(f"[RealSenseCamera] stream must be 'color' or 'depth', got {stream!r}")
        self._depth_visual_min_m = float(depth_visual_min_m)
        self._depth_visual_max_m = float(depth_visual_max_m)
        if self._depth_visual_max_m <= self._depth_visual_min_m:
            self._depth_visual_min_m = 0.15
            self._depth_visual_max_m = 3.0
        self._depth_colormap = str(depth_colormap or "turbo").lower()
        self._depth_invert = bool(depth_invert)
        self._depth_align_to_color = bool(depth_align_to_color)
        self._depth_align_fill_holes = bool(depth_align_fill_holes)
        self._depth_align_fill_iterations = max(0, int(depth_align_fill_iterations))
        self._depth_spatial_filter = bool(depth_spatial_filter)
        self._depth_temporal_filter = bool(depth_temporal_filter)
        self._depth_hole_filling = bool(depth_hole_filling)
        self._depth_hole_filling_mode = int(depth_hole_filling_mode)
        self._depth_png_params = [
            int(cv2.IMWRITE_PNG_COMPRESSION),
            max(0, min(int(depth_png_compression), 9)),
        ]
        self._latest_depth = None
        self._depth_filters = []
        self._depth_intrinsics = None
        self._color_intrinsics = None
        self._depth_to_color_extrinsics = None
        self._depth_u_grid = None
        self._depth_v_grid = None
        self._shared_rgbd = False
        self._shared_source = None
        self.pipeline = None
        self.align = None
        self._device = None
        self.g_depth_scale = 0.001
        if shared_rgbd:
            self._shared_rgbd = True
            self._shared_source = SharedRealSenseRGBDSource.acquire(rs, self._serial_number, self._img_shape, self._fps)
            if self._stream == "depth":
                self._shared_source.configure_depth_processing(
                    align_to_color=self._depth_align_to_color,
                    align_fill_holes=self._depth_align_fill_holes,
                    align_fill_iterations=self._depth_align_fill_iterations,
                    spatial_filter=self._depth_spatial_filter,
                    temporal_filter=self._depth_temporal_filter,
                    hole_filling=self._depth_hole_filling,
                    hole_filling_mode=self._depth_hole_filling_mode,
                )
                self.intrinsics = self._shared_source.color_intrinsics if self._depth_align_to_color else self._shared_source.depth_intrinsics
            else:
                self.intrinsics = self._shared_source.color_intrinsics
            self.g_depth_scale = self._shared_source.depth_scale
            logger_mp.info(str(self))
            return

        try:
            self.pipeline = rs.pipeline()
            config = rs.config()
            if self._serial_number is not None:
                config.enable_device(self._serial_number)

            if self._stream == "depth":
                config.enable_stream(rs.stream.depth, self._img_shape[1], self._img_shape[0], rs.format.z16, self._fps)
            else:
                config.enable_stream(rs.stream.color, self._img_shape[1], self._img_shape[0], rs.format.bgr8, self._fps)
            if self._stream == "color" and self._enable_depth:
                config.enable_stream(rs.stream.depth, self._img_shape[1], self._img_shape[0], rs.format.z16, self._fps)
                self.align = rs.align(rs.stream.color)

            profile = self.pipeline.start(config)
            self._device = profile.get_device()
            if self._device is None:
                logger_mp.error('[RealSenseCamera] pipe_profile.get_device() is None .')
            if self._stream == "depth" or self._enable_depth:
                assert self._device is not None
                depth_sensor = self._device.first_depth_sensor()
                self.g_depth_scale = depth_sensor.get_depth_scale()

            stream_profile = profile.get_stream(rs.stream.depth if self._stream == "depth" else rs.stream.color)
            video_profile = stream_profile.as_video_stream_profile()
            self.intrinsics = video_profile.get_intrinsics()
            if self._stream == "depth":
                self._depth_intrinsics = self.intrinsics
                self._setup_depth_filters(rs)
                if self._depth_align_to_color:
                    self._setup_manual_depth_to_color_alignment(rs, video_profile)
            logger_mp.info(str(self))
        except Exception as e:
            if self.pipeline:
                try:
                    self.pipeline.stop()
                except:
                    pass
            raise RuntimeError(f"[RealSenseCamera] Failed to initialize RealSense camera {self._serial_number}: {e}")

    def __str__(self):
        return (
            f"[RealSenseCamera: {self._cam_topic}] initialized with {self._stream} stream, "
            f"{self._img_shape[0]}x{self._img_shape[1]} @ {self._fps} FPS.\n"
            f"ZMQ: {'enabled, zmq_port=' + str(self._zmq_port) if self._enable_zmq else 'disabled'}; "
            f"WebRTC: {'enabled, webrtc_port=' + str(self._webrtc_port) if self._enable_webrtc else 'disabled'}"
        )

    def check_pyrealsense2_install(self):
        try:
            import pyrealsense2 as rs
            return rs
        except Exception as e:
            raise ImportError(
                "pyrealsense2 not installed. Install Intel RealSense SDK and pyrealsense2 Python bindings."
            ) from e

    @staticmethod
    def _call_or_attr(obj, name, default=None):
        value = getattr(obj, name, default)
        return value() if callable(value) else value

    def _find_color_video_profile(self, rs):
        target_h, target_w = self._img_shape
        fallback = None
        try:
            sensors = self._device.query_sensors()
        except Exception:
            return None

        for sensor in sensors:
            for stream_profile in sensor.get_stream_profiles():
                try:
                    if stream_profile.stream_type() != rs.stream.color:
                        continue
                    video_profile = stream_profile.as_video_stream_profile()
                    width = self._call_or_attr(video_profile, "width")
                    height = self._call_or_attr(video_profile, "height")
                    if width != target_w or height != target_h:
                        continue
                    if fallback is None:
                        fallback = video_profile
                    if self._call_or_attr(stream_profile, "fps") == self._fps:
                        return video_profile
                except Exception:
                    continue
        return fallback

    def _setup_manual_depth_to_color_alignment(self, rs, depth_video_profile):
        color_video_profile = self._find_color_video_profile(rs)
        if color_video_profile is None:
            self._depth_align_to_color = False
            logger_mp.warning(
                f"[RealSenseCamera] {self._cam_topic}: cannot find matching color intrinsics; "
                "depth-to-color alignment disabled."
            )
            return

        try:
            self._depth_intrinsics = depth_video_profile.get_intrinsics()
            self._color_intrinsics = color_video_profile.get_intrinsics()
            self._depth_to_color_extrinsics = depth_video_profile.get_extrinsics_to(color_video_profile)
            yy, xx = np.indices((self._img_shape[0], self._img_shape[1]), dtype=np.float32)
            self._depth_u_grid = xx
            self._depth_v_grid = yy
            self.intrinsics = self._color_intrinsics
            logger_mp.info(f"[RealSenseCamera] {self._cam_topic}: depth stream aligned to color intrinsics.")
        except Exception as e:
            self._depth_align_to_color = False
            logger_mp.warning(
                f"[RealSenseCamera] {self._cam_topic}: failed to set up depth-to-color alignment: {e}"
            )

    def _set_filter_option(self, filter_obj, option, value):
        try:
            if hasattr(filter_obj, "supports") and not filter_obj.supports(option):
                return
            filter_obj.set_option(option, value)
        except Exception:
            pass

    def _setup_depth_filters(self, rs):
        if self._depth_spatial_filter:
            spatial = rs.spatial_filter()
            self._set_filter_option(spatial, rs.option.filter_magnitude, 2)
            self._set_filter_option(spatial, rs.option.filter_smooth_alpha, 0.5)
            self._set_filter_option(spatial, rs.option.filter_smooth_delta, 20)
            self._set_filter_option(spatial, rs.option.holes_fill, 1)
            self._depth_filters.append(spatial)
        if self._depth_temporal_filter:
            self._depth_filters.append(rs.temporal_filter())
        if self._depth_hole_filling:
            hole_filling = rs.hole_filling_filter()
            self._set_filter_option(hole_filling, rs.option.holes_fill, self._depth_hole_filling_mode)
            self._depth_filters.append(hole_filling)

    def _apply_depth_filters(self, depth_frame):
        filtered_frame = depth_frame
        for depth_filter in self._depth_filters:
            filtered_frame = depth_filter.process(filtered_frame)
        return filtered_frame

    def _align_depth_to_color(self, depth_z16):
        if (
            not self._depth_align_to_color
            or self._depth_intrinsics is None
            or self._color_intrinsics is None
            or self._depth_to_color_extrinsics is None
        ):
            return depth_z16

        valid = depth_z16 > 0
        if not np.any(valid):
            return depth_z16

        if self._depth_u_grid is None or self._depth_u_grid.shape != depth_z16.shape:
            yy, xx = np.indices(depth_z16.shape, dtype=np.float32)
            self._depth_u_grid = xx
            self._depth_v_grid = yy

        z = depth_z16.astype(np.float32) * float(self.g_depth_scale)
        u = self._depth_u_grid[valid]
        v = self._depth_v_grid[valid]
        z_d = z[valid]

        depth_intr = self._depth_intrinsics
        color_intr = self._color_intrinsics
        x_d = (u - depth_intr.ppx) / depth_intr.fx * z_d
        y_d = (v - depth_intr.ppy) / depth_intr.fy * z_d

        extr = self._depth_to_color_extrinsics
        r = np.asarray(extr.rotation, dtype=np.float32)
        t = np.asarray(extr.translation, dtype=np.float32)
        x_c = r[0] * x_d + r[3] * y_d + r[6] * z_d + t[0]
        y_c = r[1] * x_d + r[4] * y_d + r[7] * z_d + t[1]
        z_c = r[2] * x_d + r[5] * y_d + r[8] * z_d + t[2]

        projected_valid = z_c > 0
        if not np.any(projected_valid):
            return np.zeros_like(depth_z16)

        u_c = color_intr.fx * (x_c[projected_valid] / z_c[projected_valid]) + color_intr.ppx
        v_c = color_intr.fy * (y_c[projected_valid] / z_c[projected_valid]) + color_intr.ppy
        z_c = z_c[projected_valid]

        target_h, target_w = self._img_shape
        u_i = np.rint(u_c).astype(np.int32)
        v_i = np.rint(v_c).astype(np.int32)
        in_bounds = (u_i >= 0) & (u_i < target_w) & (v_i >= 0) & (v_i < target_h)
        if not np.any(in_bounds):
            return np.zeros_like(depth_z16)

        flat_idx = v_i[in_bounds] * target_w + u_i[in_bounds]
        aligned_m = np.full(target_h * target_w, np.inf, dtype=np.float32)
        np.minimum.at(aligned_m, flat_idx, z_c[in_bounds])
        aligned_m[~np.isfinite(aligned_m)] = 0.0
        aligned_z16 = np.rint(aligned_m / float(self.g_depth_scale))
        aligned_z16 = np.clip(aligned_z16, 0, np.iinfo(np.uint16).max).astype(np.uint16)
        return aligned_z16.reshape((target_h, target_w))

    def _fill_aligned_depth_holes(self, depth_z16):
        if not self._depth_align_fill_holes or self._depth_align_fill_iterations <= 0:
            return depth_z16

        filled = depth_z16.copy()
        kernel = np.ones((3, 3), dtype=np.uint8)
        for _ in range(self._depth_align_fill_iterations):
            holes = filled == 0
            if not np.any(holes):
                break
            dilated = cv2.dilate(filled, kernel)
            filled[holes] = dilated[holes]
        return filled

    def _depth_to_bgr(self, depth_z16):
        depth_m = depth_z16.astype(np.float32) * float(self.g_depth_scale)
        valid = depth_z16 > 0
        depth_norm = (depth_m - self._depth_visual_min_m) / (self._depth_visual_max_m - self._depth_visual_min_m)
        depth_norm = np.clip(depth_norm, 0.0, 1.0)
        if self._depth_invert:
            depth_norm = 1.0 - depth_norm
        gray = (depth_norm * 255.0).astype(np.uint8)
        gray[~valid] = 0

        if self._depth_colormap in ("gray", "grey", "none"):
            return cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)

        colormap = cv2.COLORMAP_JET
        if self._depth_colormap == "turbo" and hasattr(cv2, "COLORMAP_TURBO"):
            colormap = cv2.COLORMAP_TURBO
        bgr_numpy = cv2.applyColorMap(gray, colormap)
        bgr_numpy[~valid] = (0, 0, 0)
        return bgr_numpy

    def _publish_depth_png(self, depth_z16: np.ndarray, capture_time_ns: int) -> None:
        if self._enable_webrtc:
            self._webrtc_buffer.write(self._depth_to_bgr(depth_z16))

        if self._enable_zmq:
            ok, buf = cv2.imencode(".png", depth_z16, self._depth_png_params)
            if ok:
                self._zmq_buffer.write(pack_image_packet(
                    buf.tobytes(),
                    cam_topic=self._cam_topic,
                    capture_time_ns=capture_time_ns,
                    encoding="png",
                    pixel_format="depth_z16",
                    dtype="uint16",
                    width=int(depth_z16.shape[1]),
                    height=int(depth_z16.shape[0]),
                ))
    
    def _update_frame(self):
        if self._shared_rgbd:
            capture_time_ns = time.monotonic_ns()
            if self._stream == "depth":
                self._latest_depth = self._shared_source.latest_depth()
                if self._latest_depth is None:
                    return None
                self._publish_depth_png(self._latest_depth, capture_time_ns)
                if not self._ready.is_set():
                    self._ready.set()
                return
            else:
                bgr_numpy = self._shared_source.latest_color()
                if bgr_numpy is None:
                    return None

            if self._enable_webrtc:
                self._webrtc_buffer.write(bgr_numpy)

            if self._enable_zmq:
                ok, buf = cv2.imencode(".jpg", bgr_numpy)
                if ok:
                    self._zmq_buffer.write(pack_image_packet(
                        buf.tobytes(),
                        cam_topic=self._cam_topic,
                        capture_time_ns=capture_time_ns,
                    ))

            if not self._ready.is_set():
                self._ready.set()
            return

        frames = self.pipeline.wait_for_frames()
        capture_time_ns = time.monotonic_ns()
        if self._stream == "depth":
            depth_frame = frames.get_depth_frame()
            if not depth_frame:
                return None
            depth_frame = self._apply_depth_filters(depth_frame)
            self._latest_depth = np.asanyarray(depth_frame.get_data())
            self._latest_depth = self._align_depth_to_color(self._latest_depth)
            self._latest_depth = self._fill_aligned_depth_holes(self._latest_depth)
            self._publish_depth_png(self._latest_depth, capture_time_ns)
            if not self._ready.is_set():
                self._ready.set()
            return
        else:
            aligned_frames = self.align.process(frames) if self.align is not None else frames
            color_frame = aligned_frames.get_color_frame()
            if not color_frame:
                return None

            if self._enable_depth:   
                depth_frame = aligned_frames.get_depth_frame()
                if depth_frame:
                    self._latest_depth = np.asanyarray(depth_frame.get_data())
                else:
                    self._latest_depth = None

            bgr_numpy = np.asanyarray(color_frame.get_data())

        if self._enable_webrtc:
            self._webrtc_buffer.write(bgr_numpy)

        if self._enable_zmq:
            ok, buf = cv2.imencode(".jpg", bgr_numpy)
            if ok:
                self._zmq_buffer.write(pack_image_packet(
                    buf.tobytes(),
                    cam_topic=self._cam_topic,
                    capture_time_ns=capture_time_ns,
                ))
        
        if not self._ready.is_set():
            self._ready.set()
    
    def get_depth_frame(self):
        if self._latest_depth is None:
            return None
        return self._latest_depth.tobytes()

    def release(self):
        if self._shared_rgbd:
            if self._shared_source is not None:
                self._shared_source.release_ref()
                self._shared_source = None
            logger_mp.info(f"[RealSenseCamera] Released {self._cam_topic}")
            return

        try:
            if self.pipeline is not None and hasattr(self.pipeline, "stop"):
                try:
                    self.pipeline.stop()
                except Exception as e:
                    logger_mp.warning(f"[RealSenseCamera] pipeline.stop() failed: {e}")
        except Exception:
            pass
        self.pipeline = None
        logger_mp.info(f"[RealSenseCamera] Released {self._cam_topic}")

class UVCCamera(BaseCamera):
    def __init__(self, cam_topic, uid, img_shape, fps, 
                 enable_zmq=True, zmq_port=55555, enable_webrtc=False, webrtc_port=66666, webrtc_codec=None,
                 format_name="MJPG"):
        super().__init__(cam_topic, img_shape, fps, enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec)
        import uvc
        self.uid = uid
        self._format_name = str(format_name or "MJPG").upper()
        self.cap = None
        try:
            self.cap = uvc.Capture(self.uid)
        except Exception as e:
            self.cap = None
            raise RuntimeError(f"[UVCCamera] Failed to open camera {self._cam_topic}: {e}")

        try:
            self.cap.frame_mode = self._choose_mode(self.cap, width=self._img_shape[1], height=self._img_shape[0], fps=self._fps)
            logger_mp.info(str(self))
        except Exception as e:
            self.cap = None
            raise RuntimeError(f"[UVCCamera] Failed to set mode for {self._cam_topic}: {e}")

    def __str__(self):
        return (
            f"[UVCCamera: {self._cam_topic}] initialized with "
            f"{self._img_shape[0]}x{self._img_shape[1]} @ {self._fps} FPS, {self._format_name}.\n"
            f"ZMQ: {'enabled, zmq port=' + str(self._zmq_port) if self._enable_zmq else 'disabled'}; "
            f"WebRTC: {'enabled, webrtc port=' + str(self._webrtc_port) if self._enable_webrtc else 'disabled'}"
        )

    def _choose_mode(self, cap, width=None, height=None, fps=None):
        for m in cap.available_modes:
            if m.width == width and m.height == height and m.fps == fps and m.format_name == self._format_name:
                return m
        raise ValueError(f"[UVCCamera] No matching uvc mode found for {width}x{height}@{fps} {self._format_name}")

    def _update_frame(self):
        if self.cap is not None:
            frame = self.cap.get_frame_robust() # get_frame(timeout=500)
            if frame is not None:
                capture_time_ns = time.monotonic_ns()
                if self._enable_zmq:
                    jpeg_buffer = getattr(frame, "jpeg_buffer", None)
                    if jpeg_buffer is not None:
                        self._zmq_buffer.write(pack_image_packet(
                            bytes(jpeg_buffer),
                            cam_topic=self._cam_topic,
                            capture_time_ns=capture_time_ns,
                        ))
                    elif frame.bgr is not None:
                        ok, buf = cv2.imencode(".jpg", frame.bgr)
                        if ok:
                            self._zmq_buffer.write(pack_image_packet(
                                buf.tobytes(),
                                cam_topic=self._cam_topic,
                                capture_time_ns=capture_time_ns,
                            ))

                if self._enable_webrtc:
                    if frame.bgr is not None:
                        self._webrtc_buffer.write(frame.bgr)

                if not self._ready.is_set():
                    self._ready.set()
            else:
                raise RuntimeError

    def release(self):
        # if usbhub is plugged out, calling stop_streaming and close may hang forever.
        # try:
        #     self.cap.stop_streaming()
        # except Exception:
        #     pass
        # try:
        #     self.cap.close()
        # except Exception:
        #     pass
        # self.cap = None
        logger_mp.info(f"[UVCCamera] Released {self._cam_topic}")

class OpenCVCamera(BaseCamera):
    _YUV_FOURCCS = frozenset({"YUYV", "YUY2", "UYVY"})

    @staticmethod
    def _fourcc_to_str(fourcc_int: int) -> str:
        if fourcc_int <= 0:
            return ""
        return "".join(chr((int(fourcc_int) >> (8 * i)) & 0xFF) for i in range(4)).strip("\x00")

    @staticmethod
    def _is_packed_yuv(fourcc: str) -> bool:
        return fourcc.upper() in OpenCVCamera._YUV_FOURCCS

    @staticmethod
    def _yuv_to_bgr_code(fourcc: str) -> int:
        if fourcc.upper() in ("YUYV", "YUY2"):
            return cv2.COLOR_YUV2BGR_YUY2
        return cv2.COLOR_YUV2BGR_UYVY

    @staticmethod
    def _looks_like_valid_bgr(bgr: np.ndarray) -> bool:
        if bgr is None or bgr.ndim != 3 or bgr.shape[2] != 3:
            return False
        ch_std = bgr.std(axis=(0, 1))
        means = bgr.mean(axis=(0, 1))
        if float(np.max(ch_std)) < 5.0:
            return False
        if abs(means[0] - means[1]) < 3.0 and abs(means[1] - means[2]) < 3.0:
            return False
        return True

    def __init__(self, cam_topic, video_path, img_shape, fps, 
                 enable_zmq=True, zmq_port=55555, enable_webrtc=False, webrtc_port=66666, webrtc_codec=None,
                 fourcc="MJPG"):
        super().__init__(cam_topic, img_shape, fps, enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec)
        self._video_path = video_path
        self._requested_fourcc = str(fourcc or "MJPG").upper()
        if len(self._requested_fourcc) != 4:
            raise ValueError(f"[OpenCVCamera] fourcc must be 4 characters, got {self._requested_fourcc!r}")

        self._fourcc = self._requested_fourcc
        self._opencv_rgb_convert = False
        self._needs_manual_yuv_conversion = False
        self._io_failures = 0
        self._last_reopen = 0.0
        self.cap = None
        self._open_capture()

        if not self._can_read_frame():
            self.release()
            busy = not _v4l2_can_stream_one_frame(self._video_path, self._img_shape[1], self._img_shape[0])
            hint = _v4l2_device_busy_hint(self._video_path) if busy else ""
            raise RuntimeError(
                f"[OpenCVCamera] Camera {self._cam_topic} failed to read frames from {self._video_path}. {hint}"
            )
        logger_mp.info(str(self))

    def _open_capture(self) -> None:
        self.cap = cv2.VideoCapture(self._video_path, cv2.CAP_V4L2)
        if not self.cap.isOpened():
            raise RuntimeError(
                f"[OpenCVCamera] Cannot open {self._video_path}. {_v4l2_device_busy_hint(self._video_path)}"
            )

        self.cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*self._requested_fourcc))
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self._img_shape[0])
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self._img_shape[1])
        self.cap.set(cv2.CAP_PROP_FPS, self._fps)

        actual_fourcc = self._fourcc_to_str(int(self.cap.get(cv2.CAP_PROP_FOURCC)))
        if actual_fourcc:
            if actual_fourcc != self._requested_fourcc:
                logger_mp.warning(
                    "[OpenCVCamera] %s: yaml fourcc=%s but V4L2 reports %s; using %s for decode",
                    self._cam_topic, self._requested_fourcc, actual_fourcc, actual_fourcc,
                )
            self._fourcc = actual_fourcc

        self._configure_capture_mode()

    def _reopen_capture(self) -> None:
        now = time.monotonic()
        if now - self._last_reopen < 3.0:
            return
        self._last_reopen = now
        logger_mp.warning(
            "[OpenCVCamera] %s IO failures=%d, reopening %s",
            self._cam_topic, self._io_failures, self._video_path,
        )
        if self.cap is not None:
            try:
                self.cap.release()
            except Exception:
                pass
            self.cap = None
        time.sleep(0.2)
        self._open_capture()

    def _configure_capture_mode(self) -> None:
        """Pick OpenCV RGB conversion or manual YUV decode based on device format."""
        if self._is_packed_yuv(self._fourcc):
            # RealSense YUYV：手动解码比 CAP_PROP_CONVERT_RGB 更稳（高负载/WebRTC 下少假帧）
            self.cap.set(cv2.CAP_PROP_CONVERT_RGB, 0)
            self._opencv_rgb_convert = False
            self._needs_manual_yuv_conversion = True
            logger_mp.info(
                "[OpenCVCamera] %s: manual %s -> BGR decode (RealSense/YUYV)",
                self._cam_topic, self._fourcc,
            )
            return

        self.cap.set(cv2.CAP_PROP_CONVERT_RGB, 1)
        self._opencv_rgb_convert = True
        self._needs_manual_yuv_conversion = False

    def __str__(self):
        return (
            f"[OpenCVCamera: {self._cam_topic}] initialized with "
            f"{self._img_shape[0]}x{self._img_shape[1]} @ {self._fps} FPS, {self._fourcc}.\n"
            f"ZMQ: {'enabled, zmq port=' + str(self._zmq_port) if self._enable_zmq else 'disabled'}; "
            f"WebRTC: {'enabled, webrtc port=' + str(self._webrtc_port) if self._enable_webrtc else 'disabled'}"
        )
        
    def _can_read_frame(self):
        status, _ = self._read_bgr_frame()
        return status == "ok"

    def _reshape_yuv_frame(self, frame: np.ndarray) -> Optional[np.ndarray]:
        """Pack OpenCV V4L2 YUYV buffer (often 1 x bytes) into HxWx2."""
        h, w = self._img_shape[0], self._img_shape[1]
        expected = h * w * 2
        if frame.ndim == 1:
            flat = frame
        elif frame.ndim == 2 and frame.shape[0] == 1:
            flat = frame.reshape(-1)
        elif frame.ndim == 2 and frame.shape[1] == w * 2:
            return frame.reshape(h, w, 2)
        elif frame.ndim == 3 and frame.shape[2] == 2:
            return frame
        else:
            flat = frame.reshape(-1)
        if flat.size < expected:
            return None
        return flat[:expected].reshape(h, w, 2)

    def _convert_yuv_to_bgr(self, frame: np.ndarray) -> Optional[np.ndarray]:
        if frame is None:
            return None
        try:
            if frame.ndim == 3 and frame.shape[2] == 3:
                ch_std = frame.std(axis=(0, 1))
                means = frame.mean(axis=(0, 1))
                if float(np.max(ch_std)) < 5.0 and abs(means[0] - means[1]) < 3.0 and abs(means[1] - means[2]) < 3.0:
                    return None
                return frame
            yuv = self._reshape_yuv_frame(frame)
            if yuv is None:
                return None
            return cv2.cvtColor(yuv, self._yuv_to_bgr_code(self._fourcc))
        except Exception as e:
            logger_mp.warning(f"[OpenCVCamera] YUV to BGR conversion failed: {e}")
            return None

    def _read_bgr_frame(self) -> Tuple[str, Optional[np.ndarray]]:
        """Return (status, bgr) where status is 'ok' | 'skip' | 'io'."""
        if self.cap is None:
            return "io", None
        ret, frame = self.cap.read()
        if not ret or frame is None:
            return "io", None
        if self._needs_manual_yuv_conversion:
            bgr = self._convert_yuv_to_bgr(frame)
            if bgr is not None:
                return "ok", bgr
            return "skip", None
        if self._opencv_rgb_convert:
            if self._looks_like_valid_bgr(frame):
                return "ok", frame
            bgr = self._convert_yuv_to_bgr(frame)
            if bgr is not None:
                return "ok", bgr
            return "skip", None
        if frame.ndim == 3 and frame.shape[2] == 3:
            return "ok", frame
        return "skip", None

    def _update_frame(self):
        if self.cap is None:
            return
        status, bgr_numpy = self._read_bgr_frame()
        if status == "ok" and bgr_numpy is not None:
            capture_time_ns = time.monotonic_ns()
            if self._enable_webrtc:
                self._webrtc_buffer.write(bgr_numpy)

            if self._enable_zmq:
                ok, buf = cv2.imencode(".jpg", bgr_numpy)
                if ok:
                    self._zmq_buffer.write(pack_image_packet(
                        buf.tobytes(),
                        cam_topic=self._cam_topic,
                        capture_time_ns=capture_time_ns,
                    ))

            if not self._ready.is_set():
                self._ready.set()
            self._io_failures = 0
            return

        if status == "io":
            self._io_failures += 1
            if self._io_failures >= 8:
                try:
                    self._reopen_capture()
                except Exception as exc:
                    logger_mp.warning(
                        "[OpenCVCamera] %s reopen failed: %s",
                        self._cam_topic, exc,
                    )
                self._io_failures = 0

    def release(self):
        if self.cap is not None:
            self.cap.release()
            self.cap = None
        logger_mp.info(f"[OpenCVCamera] Released {self._cam_topic}")

class ThermalCamera(BaseCamera):
    """GY-MCU90640 / MLX90640 串口热成像（依赖 irthermal 包）。"""

    def __init__(
        self,
        cam_topic,
        serial_port,
        baud,
        img_shape,
        fps,
        enable_zmq=True,
        zmq_port=55555,
        enable_webrtc=False,
        webrtc_port=66666,
        webrtc_codec=None,
        use_init=False,
        overlay=True,
        jpeg_quality=85,
        settle_s=0.12,
        read_timeout=1.0,
    ):
        try:
            from irthermal import (
                CMD_STOP,
                frame_to_temps,
                open_serial,
                poll_frame,
                sync_frame,
                temps_to_bgr,
                wake_gy_mcu,
            )
        except ImportError as exc:
            raise ImportError(
                "[ThermalCamera] irthermal 未安装。请执行: "
                "pip install -e ./IrThermal/packages/irthermal"
            ) from exc

        self._CMD_STOP = CMD_STOP
        self._frame_to_temps = frame_to_temps
        self._open_serial = open_serial
        self._poll_frame = poll_frame
        self._sync_frame = sync_frame
        self._temps_to_bgr = temps_to_bgr
        self._wake_gy_mcu = wake_gy_mcu

        super().__init__(
            cam_topic, img_shape, fps, enable_zmq, zmq_port,
            enable_webrtc, webrtc_port, webrtc_codec,
        )
        self._serial_port = serial_port
        self._baud = baud
        self._use_init = use_init
        self._overlay = overlay
        self._width = img_shape[1]
        self._height = img_shape[0]
        self._encode_params = [
            int(cv2.IMWRITE_JPEG_QUALITY),
            max(1, min(int(jpeg_quality), 100)),
        ]
        self._settle_s = max(float(settle_s), 0.05)
        self._read_timeout = max(float(read_timeout), 0.3)
        self._last_wake = time.time()
        self._fail_streak = 0
        self.ser = None

        try:
            self.ser = self._open_serial(
                serial_port, baud, use_init=use_init, timeout=self._read_timeout,
            )
            ta, temps = self._acquire_first_frame()
            logger_mp.info(
                "[ThermalCamera] %s 首帧 OK @ %s  Ta=%.1fC",
                cam_topic, serial_port, ta,
            )
            self._publish_bgr(self._render_bgr(temps, ta))
            self._ready.set()
        except Exception as exc:
            if self.ser is not None:
                try:
                    self.ser.close()
                except Exception:
                    pass
                self.ser = None
            raise RuntimeError(
                f"[ThermalCamera] Failed to initialize {cam_topic} on {serial_port}: {exc}"
            ) from exc

        logger_mp.info(str(self))

    def _acquire_first_frame(self, max_attempts: int = 5):
        """GY-MCU 非连续流：须 poll_frame（发 START）取首帧，sync_frame 易超时。"""
        last_err = None
        for attempt in range(max_attempts):
            try:
                if attempt > 0:
                    self._wake_gy_mcu(self.ser, self._baud)
                    time.sleep(0.2)
                raw = self._poll_frame(
                    self.ser,
                    settle_s=self._settle_s,
                    read_timeout=self._read_timeout,
                )
                return self._frame_to_temps(raw)
            except Exception as exc:
                last_err = exc
                logger_mp.warning(
                    "[ThermalCamera] %s 首帧尝试 %d/%d 失败: %s",
                    self._cam_topic, attempt + 1, max_attempts, exc,
                )
                time.sleep(0.3)
        raise RuntimeError(
            f"[ThermalCamera] {self._cam_topic} 首帧失败（{max_attempts} 次）: {last_err}"
        ) from last_err

    def __str__(self):
        return (
            f"[ThermalCamera: {self._cam_topic}] {self._serial_port} @ {self._baud} "
            f"{self._img_shape[0]}x{self._img_shape[1]} @ {self._fps} FPS.\n"
            f"ZMQ: {'enabled, zmq_port=' + str(self._zmq_port) if self._enable_zmq else 'disabled'}; "
            f"WebRTC: {'enabled, webrtc_port=' + str(self._webrtc_port) if self._enable_webrtc else 'disabled'}"
        )

    def _render_bgr(self, temps: np.ndarray, ta: float) -> np.ndarray:
        bgr = self._temps_to_bgr(temps, self._width, self._height)
        if self._overlay:
            label = (
                f"Ta={ta:.1f}C  min={temps.min():.1f}C  max={temps.max():.1f}C"
            )
            cv2.putText(
                bgr,
                label,
                (10, 28),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.65,
                (255, 255, 255),
                2,
            )
        return bgr

    def _publish_bgr(self, bgr: np.ndarray) -> None:
        if self._enable_webrtc:
            self._webrtc_buffer.write(bgr)
        if self._enable_zmq:
            ok, buf = cv2.imencode(".jpg", bgr, self._encode_params)
            if ok:
                self._zmq_buffer.write(pack_image_packet(
                    buf.tobytes(),
                    cam_topic=self._cam_topic,
                    capture_time_ns=time.monotonic_ns(),
                ))

    def _update_frame(self):
        if self.ser is None:
            return
        try:
            raw = self._poll_frame(
                self.ser,
                settle_s=self._settle_s,
                read_timeout=self._read_timeout,
            )
            ta, temps = self._frame_to_temps(raw)
            self._publish_bgr(self._render_bgr(temps, ta))
            if not self._ready.is_set():
                self._ready.set()
            self._fail_streak = 0
        except Exception as exc:
            self._fail_streak += 1
            if self._fail_streak in (5, 20) or self._fail_streak % 50 == 0:
                logger_mp.warning(
                    "[ThermalCamera] %s 连续 %d 帧失败: %s",
                    self._cam_topic, self._fail_streak, exc,
                )
            if time.time() - self._last_wake > 2.0:
                try:
                    self._wake_gy_mcu(self.ser, self._baud)
                    self._last_wake = time.time()
                except Exception:
                    pass

    def release(self):
        if self.ser is not None:
            try:
                self.ser.write(self._CMD_STOP)
            except Exception:
                pass
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None
        logger_mp.info(f"[ThermalCamera] Released {self._cam_topic}")

# ========================================================
# G1 local episode recorder
# ========================================================
class EncodedEpisodeWriter:
    def __init__(self, episode_dir, episode_id, text, frequency=30):
        self.episode_dir = episode_dir
        self.episode_id = int(episode_id)
        self.color_dir = os.path.join(self.episode_dir, "colors")
        self.depth_dir = os.path.join(self.episode_dir, "depths")
        self.audio_dir = os.path.join(self.episode_dir, "audios")
        self.json_path = os.path.join(self.episode_dir, "data.json")
        self.item_id = -1
        self.first_item = True
        self.finalized = False
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._worker = threading.Thread(target=self._run, daemon=True)

        os.makedirs(self.color_dir, exist_ok=True)
        os.makedirs(self.depth_dir, exist_ok=True)
        os.makedirs(self.audio_dir, exist_ok=True)

        info = {
            "version": "1.0.0",
            "date": time.strftime("%Y-%m-%d"),
            "author": "unitree",
            "image": {"width": 640, "height": 480, "fps": frequency},
            "depth": {"width": 640, "height": 480, "fps": frequency},
            "audio": {"sample_rate": 16000, "channels": 1, "format": "PCM", "bits": 16},
            "joint_names": {"left_arm": [], "left_ee": [], "right_arm": [], "right_ee": [], "body": []},
            "tactile_names": {"left_ee": [], "right_ee": []},
            "sim_state": "",
        }
        with open(self.json_path, "w", encoding="utf-8") as f:
            f.write("{\n")
            f.write('"info": ' + json.dumps(info, ensure_ascii=False, indent=4) + ",\n")
            f.write('"text": ' + json.dumps(text, ensure_ascii=False, indent=4) + ",\n")
            f.write('"data": [\n')

        self._worker.start()

    def add_item(self, colors, depths, states, actions, tactiles=None, audios=None, sim_state=None):
        self._queue.put({
            "colors": colors or {},
            "depths": depths or {},
            "states": states,
            "actions": actions,
            "tactiles": tactiles,
            "audios": audios,
            "sim_state": sim_state,
        })

    def stop(self):
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        self._queue.put(None)

    def wait_finalized(self, timeout=None):
        self._worker.join(timeout=timeout)
        return self.finalized

    def _write_encoded_map(self, base_dir, idx, data_map):
        result = {}
        for key, item in data_map.items():
            payload = item.get("payload")
            ext = item.get("ext")
            if payload is None or ext is None:
                continue
            name = f"{idx:06d}_{key}.{ext}"
            path = os.path.join(base_dir, name)
            with open(path, "wb") as f:
                f.write(payload)
            result[key] = os.path.join(os.path.basename(base_dir), name)
        return result

    def _append_json_item(self, item_data):
        with open(self.json_path, "a", encoding="utf-8") as f:
            if not self.first_item:
                f.write(",\n")
            f.write(json.dumps(item_data, ensure_ascii=False, indent=4))
            self.first_item = False

    def _run(self):
        while True:
            item = self._queue.get()
            if item is None:
                self._queue.task_done()
                break
            try:
                self.item_id += 1
                idx = self.item_id
                item_data = {
                    "idx": idx,
                    "colors": self._write_encoded_map(self.color_dir, idx, item.get("colors", {})),
                    "depths": self._write_encoded_map(self.depth_dir, idx, item.get("depths", {})),
                    "states": item.get("states"),
                    "actions": item.get("actions"),
                    "tactiles": item.get("tactiles"),
                    "audios": item.get("audios"),
                    "sim_state": item.get("sim_state"),
                }
                self._append_json_item(item_data)
            except Exception as exc:
                logger_mp.error("[EncodedEpisodeWriter] failed to write item: %s", exc)
            finally:
                self._queue.task_done()

        self._queue.join()
        with open(self.json_path, "a", encoding="utf-8") as f:
            f.write("\n]\n}")
        self.finalized = True
        logger_mp.info("[EncodedEpisodeWriter] finalized %s", self.episode_dir)


class G1EpisodeRecorderServer:
    def __init__(self, image_server, root_dir, host="0.0.0.0", port=60010):
        self._image_server = image_server
        self._root_dir = os.path.expanduser(root_dir)
        self._host = host
        self._port = int(port)
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REP)
        self._socket.bind(f"tcp://{self._host}:{self._port}")
        self._running = True
        self._active_writer = None
        self._writers = {}
        self._lock = threading.Lock()
        os.makedirs(self._root_dir, exist_ok=True)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger_mp.info("[G1EpisodeRecorder] listening at %s:%d, root=%s", self._host, self._port, self._root_dir)

    def _packet_to_encoded(self, packet, default_ext):
        if packet is None:
            return None
        metadata, payload = unpack_image_packet(packet)
        encoding = str(metadata.get("encoding", "")).lower()
        pixel_format = str(metadata.get("pixel_format", "")).lower()
        ext = "png" if encoding == "png" or "depth" in pixel_format or "z16" in pixel_format else default_ext
        return {"payload": payload, "ext": ext}

    def _capture_images(self):
        colors = {}
        depths = {}
        topic_to_color_key = {
            "head_camera": "color_0",
            "left_wrist_camera": "color_1",
            "right_wrist_camera": "color_2",
        }
        for topic, key in topic_to_color_key.items():
            cam = self._image_server._cameras.get(topic)
            if cam is None or not cam.enable_zmq():
                continue
            encoded = self._packet_to_encoded(cam.get_jpeg_bytes(), "jpg")
            if encoded is not None:
                colors[key] = encoded

        cam = self._image_server._cameras.get("depth_camera")
        if cam is not None and cam.enable_zmq():
            encoded = self._packet_to_encoded(cam.get_jpeg_bytes(), "png")
            if encoded is not None:
                depths["depth_0"] = encoded
        return colors, depths

    def _handle_start(self, payload):
        episode_id = int(payload["episode_id"])
        text = {
            "goal": payload.get("task_goal") or "Pick up the red cup on the table.",
            "desc": payload.get("task_desc") or "task description",
            "steps": payload.get("task_steps") or "step1: do this; step2: do that; ...",
        }
        episode_dir = os.path.join(self._root_dir, f"episode_{episode_id:04d}")
        with self._lock:
            if self._active_writer is not None:
                raise RuntimeError("an episode is already active")
            if os.path.exists(episode_dir):
                raise RuntimeError(f"episode already exists in tmp_data: {episode_dir}")
            writer = EncodedEpisodeWriter(
                episode_dir,
                episode_id,
                text,
                frequency=int(float(payload.get("frequency", 30))),
            )
            self._writers[episode_id] = writer
            self._active_writer = writer
        return {"ok": True, "episode_id": episode_id, "episode_path": episode_dir}

    def _handle_add_item(self, payload):
        episode_id = int(payload.get("episode_id", -1))
        with self._lock:
            writer = self._active_writer
        if writer is None:
            return {"ok": True, "dropped": True}
        if writer.episode_id != episode_id:
            return {"ok": True, "dropped": True, "reason": "stale_episode_id"}
        colors, depths = self._capture_images()
        writer.add_item(
            colors=colors,
            depths=depths,
            states=payload.get("states"),
            actions=payload.get("actions"),
            tactiles=payload.get("tactiles"),
            audios=payload.get("audios"),
            sim_state=payload.get("sim_state"),
        )
        return {"ok": True}

    def _handle_stop(self, payload):
        episode_id = int(payload["episode_id"])
        with self._lock:
            writer = self._writers.get(episode_id)
            if self._active_writer is writer:
                self._active_writer = None
        if writer is None:
            raise RuntimeError(f"unknown episode_id {episode_id}")
        writer.stop()
        return {"ok": True, "episode_id": episode_id, "episode_path": writer.episode_dir}

    def _handle_status(self, payload):
        episode_id = int(payload["episode_id"])
        writer = self._writers.get(episode_id)
        if writer is None:
            return {"ok": True, "episode_id": episode_id, "finalized": False, "missing": True}
        return {
            "ok": True,
            "episode_id": episode_id,
            "episode_path": writer.episode_dir,
            "finalized": bool(writer.finalized),
        }

    def _handle_delete(self, payload):
        episode_id = int(payload["episode_id"])
        with self._lock:
            writer = self._writers.get(episode_id)
            if writer is None:
                return {"ok": True, "missing": True}
            if not writer.finalized:
                raise RuntimeError(f"episode_{episode_id:04d} is not finalized")
            self._writers.pop(episode_id, None)
        import shutil
        shutil.rmtree(writer.episode_dir, ignore_errors=True)
        return {"ok": True, "episode_id": episode_id}

    def _dispatch(self, payload):
        cmd = payload.get("cmd")
        if cmd == "start_episode":
            return self._handle_start(payload)
        if cmd == "add_item":
            return self._handle_add_item(payload)
        if cmd == "stop_episode":
            return self._handle_stop(payload)
        if cmd == "status":
            return self._handle_status(payload)
        if cmd == "delete_episode":
            return self._handle_delete(payload)
        if cmd == "ping":
            return {"ok": True}
        raise RuntimeError(f"unknown recorder command {cmd!r}")

    def _run(self):
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        while self._running:
            try:
                socks = dict(poller.poll(timeout=200))
                if self._socket not in socks:
                    continue
                payload = self._socket.recv_json()
                try:
                    reply = self._dispatch(payload)
                except Exception as exc:
                    logger_mp.error("[G1EpisodeRecorder] command failed: %s", exc)
                    reply = {"ok": False, "error": str(exc)}
                self._socket.send_json(reply)
            except zmq.ZMQError:
                if self._running:
                    logger_mp.exception("[G1EpisodeRecorder] ZMQ error")
                break

    def stop(self):
        with self._lock:
            writers = list(self._writers.values())
            self._active_writer = None
        for writer in writers:
            if not writer.finalized:
                try:
                    writer.stop()
                    writer.wait_finalized()
                except Exception as exc:
                    logger_mp.error("[G1EpisodeRecorder] failed to finalize %s: %s", writer.episode_dir, exc)
        self._running = False
        self._thread.join(timeout=1.0)
        try:
            self._socket.close()
            self._context.term()
        except Exception:
            pass

# ========================================================
# image server
# ========================================================
class ImageServer:
    def __init__(self, cam_config, realsense_enable=False, camera_finder_verbose=False, record_root="~/tmp_data", record_port=60010, enable_record_server=True):
        self._cam_config = cam_config
        self._realsense_enable = realsense_enable
        self._stop_event = threading.Event()
        self._cameras: dict[str, BaseCamera] = {}
        self._record_server = None
        self._cam_finder = CameraFinder(realsense_enable, camera_finder_verbose)
        self._responser = ZMQ_Responser(self._cam_config)
        self._zmq_publisher_manager = ZMQ_PublisherManager.get_instance()
        self._webrtc_publisher_manager = WebRTC_PublisherManager.get_instance()
        self._publisher_threads = []  # keep references for graceful join

        try:
            # Load cameras from self.cam_config
            for cam_topic, cam_cfg in self._cam_config.items():
                if not cam_cfg.get("enable_zmq", False) and not cam_cfg.get("enable_webrtc", False):
                    continue

                enable_zmq = cam_cfg.get("enable_zmq", False)
                zmq_port = cam_cfg.get("zmq_port", None)
                enable_webrtc = cam_cfg.get("enable_webrtc", False)
                webrtc_port = cam_cfg.get("webrtc_port", None)
                webrtc_codec = cam_cfg.get("webrtc_codec", None)
                cam_type = cam_cfg.get("type", "uvc").lower()
                img_shape = cam_cfg.get("image_shape", None)
                fps = cam_cfg.get("fps", 30)
                fourcc = cam_cfg.get("fourcc", "MJPG")
                video_id = cam_cfg.get("video_id", "0")
                video_path = f"/dev/video{video_id}" if video_id else None
                physical_path = str(cam_cfg.get("physical_path")) if cam_cfg.get("physical_path") else None
                serial_number = str(cam_cfg.get("serial_number")) if cam_cfg.get("serial_number") else None

                if cam_type == "opencv":
                    if physical_path is not None:
                        vpath = self._cam_finder.get_vpath_by_ppath(physical_path)
                        if vpath is None:
                            self._cameras[cam_topic] = None
                            logger_mp.error(f"[Image Server] Cannot find OpenCVCamera for {cam_topic} with physical path {physical_path}")
                        else:
                            self._cameras[cam_topic] = OpenCVCamera(cam_topic, vpath, img_shape, fps, 
                                                                    enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec,
                                                                    fourcc=fourcc)
                            continue

                    if serial_number is not None:
                        vpath = self._cam_finder.get_vpath_by_sn(serial_number)
                        if vpath is None:
                            self._cameras[cam_topic] = None
                            logger_mp.error(f"[Image Server] Cannot find OpenCVCamera for {cam_topic} with serial number {serial_number}")
                        else:
                            self._cameras[cam_topic] = OpenCVCamera(cam_topic, vpath, img_shape, fps, 
                                                                    enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec,
                                                                    fourcc=fourcc)
                        # once you specify either `physical_path` or `serial_number`, the system will no longer fall back to searching by `video_id`.
                        # ——— even if no camera matches the given path/serial.
                        continue
                    
                    resolved_path = self._cam_finder.resolve_opencv_video_path(
                        video_path, serial_number, physical_path
                    )
                    if resolved_path is None or not self._cam_finder.is_vpath_exist(resolved_path):
                        self._cameras[cam_topic] = None
                        logger_mp.error(f"[Image Server] Cannot find OpenCVCamera for {cam_topic} with video_id {video_id}")
                    elif not self._cam_finder.is_rgb_vpath(resolved_path):
                        self._cameras[cam_topic] = None
                        logger_mp.error(
                            f"[Image Server] {resolved_path} is not a V4L2 RGB node for {cam_topic} "
                            f"(depth/IR nodes can look like 3-channel in OpenCV). "
                            f"Run `teleimager-server --cf` after freeing the camera."
                        )
                    else:
                        self._cameras[cam_topic] = OpenCVCamera(
                            cam_topic, resolved_path, img_shape, fps,
                            enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec,
                            fourcc=fourcc,
                        )
                        

                elif cam_type == "realsense":
                    if not self._realsense_enable:
                        self._cameras[cam_topic] = None
                        logger_mp.error(f"[Image Server] Please start image server with the '--rs' flag to support Realsense {cam_topic}.")
                    elif serial_number is not None and not self._cam_finder.is_rs_serial_exist(serial_number):
                        self._cameras[cam_topic] = None
                        logger_mp.error(f"[Image Server] Cannot find RealSenseCamera for {cam_topic} with serial number {serial_number}")
                    elif serial_number is None and not self._cam_finder.rs_serial_numbers:
                        self._cameras[cam_topic] = None
                        logger_mp.error(f"[Image Server] Cannot find any RealSenseCamera for {cam_topic}")
                    else:
                        rs_serial_number = serial_number or self._cam_finder.rs_serial_numbers[0]
                        self._cameras[cam_topic] = RealSenseCamera(
                            cam_topic, rs_serial_number, img_shape, fps,
                            enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec,
                            enable_depth=bool(cam_cfg.get("enable_depth", False)),
                            stream=cam_cfg.get("stream", "color"),
                            depth_visual_min_m=cam_cfg.get("depth_visual_min_m", 0.15),
                            depth_visual_max_m=cam_cfg.get("depth_visual_max_m", 3.0),
                            depth_colormap=cam_cfg.get("depth_colormap", "turbo"),
                            depth_invert=cam_cfg.get("depth_invert", False),
                            depth_align_to_color=cam_cfg.get("depth_align_to_color", False),
                            depth_align_fill_holes=cam_cfg.get("depth_align_fill_holes", False),
                            depth_align_fill_iterations=cam_cfg.get("depth_align_fill_iterations", 1),
                            depth_spatial_filter=cam_cfg.get("depth_spatial_filter", False),
                            depth_temporal_filter=cam_cfg.get("depth_temporal_filter", False),
                            depth_hole_filling=cam_cfg.get("depth_hole_filling", False),
                            depth_hole_filling_mode=cam_cfg.get("depth_hole_filling_mode", 1),
                            shared_rgbd=cam_cfg.get("shared_rgbd", False),
                            depth_png_compression=cam_cfg.get("depth_png_compression", 1),
                        )

                elif cam_type == "uvc":
                    uid = None
                    if physical_path is not None:
                        uid = self._cam_finder.get_uid_by_ppath(physical_path)
                        if uid is None:
                            self._cameras[cam_topic] = None
                            logger_mp.error(f"[Image Server] Cannot find UVCCamera for {cam_topic} with physical path {physical_path}")
                        else:
                            self._cameras[cam_topic] = UVCCamera(cam_topic, uid, img_shape, fps, 
                                                                 enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec,
                                                                 format_name=fourcc)
                            continue

                    if serial_number is not None:
                        uid = self._cam_finder.get_uid_by_sn(serial_number)
                        if uid is None:
                            self._cameras[cam_topic] = None
                            logger_mp.error(f"[Image Server] Cannot find UVCCamera for {cam_topic} with serial number {serial_number}")
                        else:
                            self._cameras[cam_topic] = UVCCamera(cam_topic, uid, img_shape, fps, 
                                                                 enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec,
                                                                 format_name=fourcc)
                        # once you specify either `physical_path` or `serial_number`, the system will no longer fall back to searching by `video_id`.
                        # ——— even if no camera matches the given path/serial.
                        continue

                    if video_id is not None:
                        if not self._cam_finder.is_vpath_exist(video_path):
                            self._cameras[cam_topic] = None
                            logger_mp.error(f"[Image Server] Cannot find UVCCamera for {cam_topic} with video_id {video_id}")
                        else:
                            uid = self._cam_finder.get_uid_by_vpath(video_path)
                            if uid is None:
                                self._cameras[cam_topic] = None
                                logger_mp.error(f"[Image Server] Cannot find UVCCamera for {cam_topic} with uid from video_id {video_id}")
                            else:
                                try:
                                    self._cameras[cam_topic] = UVCCamera(
                                        cam_topic, uid, img_shape, fps,
                                        enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec,
                                        format_name=fourcc,
                                    )
                                except RuntimeError as exc:
                                    logger_mp.warning(
                                        "[Image Server] UVCCamera failed for %s on %s (%s); falling back to OpenCV/V4L2.",
                                        cam_topic,
                                        video_path,
                                        exc,
                                    )
                                    self._cameras[cam_topic] = OpenCVCamera(
                                        cam_topic, video_path, img_shape, fps,
                                        enable_zmq, zmq_port, enable_webrtc, webrtc_port, webrtc_codec,
                                        fourcc=fourcc,
                                    )

                elif cam_type == "thermal":
                    if img_shape is None:
                        img_shape = [480, 640]
                    serial_port = str(cam_cfg.get("serial_port", "/dev/ttyUSB0"))
                    baud = int(cam_cfg.get("baud", 460800))
                    use_init = bool(cam_cfg.get("use_init", False))
                    overlay = bool(cam_cfg.get("overlay", True))
                    jpeg_quality = int(cam_cfg.get("jpeg_quality", 85))
                    settle_s = float(cam_cfg.get("settle_s", 0.12))
                    read_timeout = float(cam_cfg.get("read_timeout", 1.0))
                    optional = bool(cam_cfg.get("optional", True))
                    try:
                        self._cameras[cam_topic] = ThermalCamera(
                            cam_topic,
                            serial_port,
                            baud,
                            img_shape,
                            fps,
                            enable_zmq,
                            zmq_port,
                            enable_webrtc,
                            webrtc_port,
                            webrtc_codec,
                            use_init=use_init,
                            overlay=overlay,
                            jpeg_quality=jpeg_quality,
                            settle_s=settle_s,
                            read_timeout=read_timeout,
                        )
                    except Exception as exc:
                        if optional:
                            logger_mp.warning(
                                "[Image Server] Optional thermal %s disabled: %s",
                                cam_topic, exc,
                            )
                            continue
                        raise

                else:
                    logger_mp.error(f"[Image Server] Unknown camera type {cam_type} for {cam_topic}, skipping...")
                    continue
        except Exception as e:
            logger_mp.error(f"[Image Server] Initialization failed: {e}")
            self._clean_up()
            raise

        if enable_record_server:
            self._record_server = G1EpisodeRecorderServer(self, root_dir=record_root, port=record_port)

        logger_mp.info("[Image Server] Image server has started, waiting for client connections...")

    def _update_frames(self, cam_topic: str, camera: BaseCamera):
        try:
            interval = 1.0 / camera.get_fps()
            next_frame_time = time.monotonic()
            while not self._stop_event.is_set():
                try:
                    camera._update_frame()
                except Exception as e:
                    logger_mp.error(
                        f"[Image Server] Error updating frame for {cam_topic} camera: {e}"
                    )
                    self._stop_event.set()
                    break
                next_frame_time += interval
                sleep_time = next_frame_time - time.monotonic()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    next_frame_time = time.monotonic()
        except Exception as e:
            logger_mp.error(f"[Image Server] Failed to update frames for {cam_topic} camera: {e}")
            self._stop_event.set()

    def _zmq_pub(self, cam_topic: str, camera: BaseCamera):
        try:
            interval = 1.0 / camera.get_fps()
            next_frame_time = time.monotonic()

            while not self._stop_event.is_set():
                jpeg_bytes = camera.get_jpeg_bytes()
                if jpeg_bytes is not None:
                    self._zmq_publisher_manager.publish(jpeg_bytes, camera.get_zmq_port())
                else:
                    time.sleep(0.01)
                    continue

                next_frame_time += interval
                sleep_time = next_frame_time - time.monotonic()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    next_frame_time = time.monotonic()
        except Exception as e:
            logger_mp.error(f"[Image Server] Failed to publish zmq frame from {cam_topic} camera.")
            self._stop_event.set()
    
    def _webrtc_pub(self, cam_topic: str, camera: BaseCamera):
        try:
            interval = 1.0 / camera.get_fps()
            webrtc_codec = camera.get_webrtc_codec()
            next_frame_time = time.monotonic()
            while not self._stop_event.is_set():
                bgr_frame = camera.get_bgr_frame()

                if bgr_frame is not None:
                    self._webrtc_publisher_manager.publish(bgr_frame, camera.get_webrtc_port(), codec_pref=webrtc_codec)
                else:
                    time.sleep(0.01)
                    continue

                next_frame_time += interval
                sleep_time = next_frame_time - time.monotonic()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    next_frame_time = time.monotonic()
        except Exception as e:
            logger_mp.error(f"[Image Server] Failed to publish rtc frame from {cam_topic} camera.")
            self._stop_event.set()

    def _clean_up(self):
        if self._record_server is not None:
            try:
                self._record_server.stop()
            except Exception:
                pass
            self._record_server = None
        self._responser.stop()
        for t in self._publisher_threads:
            if t.is_alive():
                t.join(timeout=1.0)
        self._publisher_threads.clear()
        
        try:
            self._zmq_publisher_manager.close()
        except Exception:
            pass
        try:
            self._webrtc_publisher_manager.close()
        except Exception:
            pass

        for cam in self._cameras.values():
            if cam:
                try:
                    cam.release()
                except Exception as e:
                    logger_mp.error(f"[Image Server] Error releasing camera {cam._cam_topic}: {e}")
        logger_mp.info("[Image Server] Clean up completed. Server stopped.")

    # --------------------------------------------------------
    # public api
    # --------------------------------------------------------
    def start(self):
        for camera_topic, camera in self._cameras.items():
            if camera is None:
                logger_mp.error(f"[Image Server] Camera {camera_topic} failed to initialize previously, cannot start.")
                self._stop_event.set()
                self._clean_up()
                return
            t = threading.Thread(target=self._update_frames, args=(camera_topic, camera), daemon=True)
            t.start()
            self._publisher_threads.append(t)
        
        for camera_topic, camera in self._cameras.items():
            ready = camera.wait_until_ready(timeout=5.0)
            if not ready:
                logger_mp.error(f"[Image Server] {camera_topic} ready timeout.")
                self._stop_event.set()
                self._clean_up()
            logger_mp.info(f"[Image Server] {camera_topic} is ready.")
        
        for camera_topic, camera in self._cameras.items():
            if camera.enable_webrtc():
                t = threading.Thread(target=self._webrtc_pub, args=(camera_topic, camera), daemon=True)
                t.start()
                self._publisher_threads.append(t)

            if camera.enable_zmq():
                t = threading.Thread(target=self._zmq_pub, args=(camera_topic, camera), daemon=True)
                t.start()
                self._publisher_threads.append(t)

    def wait(self):
        self._stop_event.wait()
        self._clean_up()

    def stop(self):
        self._stop_event.set()

# ========================================================
# utility functions
# ========================================================
def signal_handler(server, signum, frame):
    logger_mp.info(f"[Image Server] Received signal {signum}, initiating graceful shutdown...")
    server.stop()

def main():
    logger_mp.info(
        "\n====================== Image Server Startup Guide ======================\n"
        "Please first read this repo's README.md to learn how to configure and use the teleimager.\n"
        "To discover connected cameras, run the following command:\n"
        "\n"
        "    teleimager-server --cf\n"
        "\n"
        "The '--cf' flag means 'camera find'.\n"
        "This will list all detected cameras and their details (video paths, serial numbers and physical path etc.).\n"
        "Use that information to fill in your 'cam_config_server_from_G1.yaml' file.\n"
        "Once configured, you can start the image server with:\n"
        "\n"
        "    teleimager-server\n"
        "\n"
        "Note:\n"
        " - If you have RealSense cameras, add the '--rs' flag to enable RealSense support.\n"
        " - Make sure you have proper permissions to access the camera devices (e.g., run with sudo or set udev rules).\n"
        "=========================================================================="
    )

    # command line args
    parser = argparse.ArgumentParser()
    parser.add_argument('--cf', action = 'store_true', help = 'Enable camera found mode, print all connected cameras info')
    parser.add_argument('--rs', action = 'store_true', help = 'Enable RealSense camera mode. Otherwise only find UVC/OpenCV cameras.')
    parser.add_argument('--record-root', type=str, default='~/tmp_data', help='G1-local temporary episode root directory.')
    parser.add_argument('--record-port', type=int, default=60010, help='G1-local episode recorder command port.')
    parser.add_argument('--disable-record-server', action='store_true', help='Disable G1-local episode recorder server.')
    args = parser.parse_args()

    # if enable camera finder mode, just print cameras info and exit
    if args.cf:
        cf = CameraFinder(realsense_enable=args.rs, verbose=True)
        print("Found video devices:", cf.video_paths)
        print("Found V4L2 RGB video devices:", cf.uvc_rgb_video_paths)
        for vpath in cf.uvc_rgb_video_paths:
            stream_ok = _v4l2_can_stream_one_frame(vpath)
            print(f"  {vpath}: stream_test={'OK' if stream_ok else 'BUSY/FAILED'}")
            if not stream_ok:
                print(f"    -> {_v4l2_device_busy_hint(vpath)}")
        exit(0)

    # Load config file, start image server
    try:
        with open(CONFIG_PATH, "r") as f:
            cam_config = yaml.safe_load(f)
    except Exception as e:
        logger_mp.error(f"Failed to load configuration file at {CONFIG_PATH}: {e}")
        exit(1)

    # start image server
    server = ImageServer(
        cam_config,
        realsense_enable=args.rs,
        camera_finder_verbose=False,
        record_root=args.record_root,
        record_port=args.record_port,
        enable_record_server=not args.disable_record_server,
    )
    server.start()

    # graceful shutdown handling
    signal.signal(signal.SIGINT, functools.partial(signal_handler, server))
    signal.signal(signal.SIGTERM, functools.partial(signal_handler, server))

    logger_mp.info("[Image Server] Running... Press Ctrl+C to exit.")
    server.wait()

    # usbhub plugout may cause block process exit, no better solution for now
    time.sleep(0.5)
    os.killpg(os.getpgrp(), 9)

if __name__ == "__main__":
    main()
