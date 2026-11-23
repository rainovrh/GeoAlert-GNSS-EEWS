#!/usr/bin/env python3
"""
GeoAlert - Real-Time GNSS Ground Displacement & Earthquake Early Warning System
-------------------------------------------------------------------------------
Stasiun Pantau: GALT-BDG-01 (Stasiun Referensi Geodesi Bandung Utara)
Target GNSS Stream: 100.122.238.102:9000 (RTKLIB str2str TCP Stream)
Ambang Batas Peringatan: 10.0 Centimeter (0.10 Meter)

Layanan Aktif:
1. Resilient Async TCP Socket Client: Mendengarkan aliran data nyata GNSS RTK dari RTKLIB.
2. RTK Quality Gatekeeper & Strict Calibration:
   - Wajib 60 sampel RTK FIXED (Q=1) konsisten untuk kalibrasi baseline awal.
   - Jika terhalang FLOAT/non-FIX, kalibrasi diulang dari awal [1/60].
   - Setelah baseline terkunci, data FLOAT ditahan; hanya data FIXED yang diproses.
3. Asyncio WebSocket Server: Menyiarkan telemetri dan alert darurat ke dashboard di ws://localhost:8765.
4. Non-Blocking Async WhatsApp Worker: Mengirim notifikasi darurat via Fonnte REST API dengan cooldown 60s.

Author: Expert IoT & Geodetic Seismology Engineer
"""

import asyncio
import datetime
import json
import math
import os
import sys
import time
from typing import Any, Callable, Coroutine, List, Optional, Set, Tuple

# Muat variabel lingkungan dari .env jika tersedia
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Verifikasi library websockets
try:
    import websockets
except ImportError:
    print("\n❌ Error: Paket 'websockets' belum terinstall.")
    print("👉 Silakan jalankan perintah: pip install websockets\n")
    sys.exit(1)

try:
    import aiohttp
except ImportError:
    aiohttp = None


# ==============================================================================
# KONFIGURASI SISTEM (DATA REAL)
# ==============================================================================
# 1. Aliran Data TCP GNSS (Node Tailscale RTKLIB)
TCP_IP: str = os.getenv("TCP_IP", "100.122.238.102")
TCP_PORT: int = int(os.getenv("TCP_PORT", "9000"))
RECONNECT_DELAY_SEC: float = float(os.getenv("RECONNECT_DELAY_SEC", "3.0"))
SOCKET_TIMEOUT_SEC: float = float(os.getenv("SOCKET_TIMEOUT_SEC", "10.0"))

# 2. Pengaturan Kualitas Solusi RTK & Kalibrasi
# Q=1: FIX, Q=2: FLOAT, Q=3: SBAS, Q=4: DGPS, Q=5: SINGLE, Q=6: PPP
REQUIRE_FIXED_ONLY: bool = os.getenv("REQUIRE_FIXED_ONLY", "True").lower() in ("true", "1", "yes")
BASELINE_SAMPLE_COUNT: int = int(os.getenv("BASELINE_SAMPLE_COUNT", "60"))
DISPLACEMENT_THRESHOLD_CM: float = float(os.getenv("DISPLACEMENT_THRESHOLD_CM", "10.0"))
DISPLACEMENT_THRESHOLD_M: float = DISPLACEMENT_THRESHOLD_CM / 100.0
HAZARD_DURATION_SEC: float = float(os.getenv("HAZARD_DURATION_SEC", "60.0"))
SETTLE_MAX_JITTER_M: float = float(os.getenv("SETTLE_MAX_JITTER_M", "0.05"))
STATION_ID: str = os.getenv("STATION_ID", "GALT-BDG-01")

Q_NAME_MAP = {
    1: "FIX",
    2: "FLOAT",
    3: "SBAS",
    4: "DGPS",
    5: "SINGLE",
    6: "PPP"
}

# 3. Server WebSocket Broadcast
WS_HOST: str = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT: int = int(os.getenv("WS_PORT", "8765"))

# 4. Gateway Notifikasi WhatsApp (Fonnte)
WA_PROVIDER: str = os.getenv("WA_PROVIDER", "FONNTE")
FONNTE_TOKEN: str = os.getenv("FONNTE_TOKEN", "EwNPXCnx731JZbzpiMtF")
WA_TARGET_NUMBER: str = os.getenv("WA_TARGET_NUMBER", "62895708129090")
WA_COOLDOWN_SEC: float = float(os.getenv("WA_COOLDOWN_SEC", "60.0"))  # Cooldown 60s agar stakeholder tidak di-spam saat gempa bergetar


# ==============================================================================
# WHATSAPP ASYNC WORKER & COOLDOWN TRACKER
# ==============================================================================
class AlertCooldownTracker:
    """Mengelola jeda cooldown pengiriman WhatsApp agar tidak spam saat getaran berulang."""
    def __init__(self, cooldown_sec: float = WA_COOLDOWN_SEC) -> None:
        self.cooldown_sec = cooldown_sec
        self.last_alert_time: float = 0.0

    def can_send(self) -> bool:
        return (time.time() - self.last_alert_time) >= self.cooldown_sec

    def mark_sent(self) -> None:
        self.last_alert_time = time.time()

    def time_remaining(self) -> float:
        return max(0.0, self.cooldown_sec - (time.time() - self.last_alert_time))


async def send_whatsapp_alert(
    delta_cm: float,
    timestamp: str,
    coords: Tuple[float, float, float]
) -> None:
    """Mengirim pesan peringatan darurat ke WhatsApp stakeholder via REST API secara asinkron."""
    if not FONNTE_TOKEN or not WA_TARGET_NUMBER:
        return

    if aiohttp is None:
        print("❌ [WhatsApp] Library 'aiohttp' belum terinstall (pip install aiohttp).")
        return

    raw_digits = "".join(c for c in WA_TARGET_NUMBER if c.isdigit())
    num_standard = "62" + raw_digits[1:] if raw_digits.startswith("0") else raw_digits

    coords_formatted = f"X: {coords[0]:.3f}, Y: {coords[1]:.3f}, Z: {coords[2]:.3f}"

    message_text = (
        f"🚨 *ALARM BENCANA: PERGESERAN TANAH TERDETEKSI!* 🚨\n\n"
        f"*Stasiun Pantau:* {STATION_ID}\n"
        f"*Waktu Kejadian:* {timestamp}\n"
        f"*Besar Pergeseran:* {delta_cm:.1f} cm\n"
        f"*Posisi Terkini (X,Y,Z):* {coords_formatted}\n\n"
        f"Segera pantau dashboard darurat:\n"
        f"🔗 https://geoalert-dashboard.local"
    )

    timeout = aiohttp.ClientTimeout(total=12)
    url = "https://api.fonnte.com/send"
    headers = {"Authorization": FONNTE_TOKEN.strip()}
    post_data = {
        "target": num_standard,
        "message": message_text,
        "countryCode": "62"
    }

    print(f"📲 [Fonnte WA] Mengirim notifikasi darurat ({delta_cm:.1f} cm) ke {num_standard}...")

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, data=post_data) as response:
                try:
                    resp_data = await response.json()
                except Exception:
                    resp_data = {"raw": await response.text()}

                if response.status == 200 and resp_data.get("status") is True:
                    print(f"✅ [Fonnte WA] Notifikasi darurat BERHASIL terkirim ke {num_standard}!")
                else:
                    reason = resp_data.get("reason") or resp_data.get("detail") or str(resp_data)
                    print(f"⚠️ [Fonnte WA] Gagal mengirim: {reason}")
    except Exception as e:
        print(f"⚠️ [Fonnte WA] Kesalahan koneksi WhatsApp: {e}")


# ==============================================================================
# WEBSOCKET MANAGER
# ==============================================================================
class WebSocketManager:
    """Mengelola koneksi klien WebSocket frontend dashboard."""
    def __init__(self) -> None:
        self.active_connections: Set[Any] = set()

    async def register(self, websocket: Any) -> None:
        self.active_connections.add(websocket)
        print(f"🔌 [WebSocket] Frontend terhubung. Total klien aktif: {len(self.active_connections)}")

    async def unregister(self, websocket: Any) -> None:
        self.active_connections.discard(websocket)
        print(f"🔌 [WebSocket] Frontend terputus. Sisa klien aktif: {len(self.active_connections)}")

    async def broadcast(self, payload: dict) -> None:
        if not self.active_connections:
            return
        message = json.dumps(payload)
        stale = set()
        for ws in self.active_connections:
            try:
                await ws.send(message)
            except Exception:
                stale.add(ws)
        for ws in stale:
            self.active_connections.discard(ws)


# ==============================================================================
# PARSER DAN UTILITAS SPASIAL GEODESI
# ==============================================================================
def parse_gnss_line(raw_line: str) -> Optional[Tuple[str, float, float, float, int, int]]:
    """
    Mem-parsing baris output stream RTKLIB (format comma-delimited atau space-delimited).
    Mengembalikan: (timestamp, x, y, z, q_flag, num_sats) atau None jika header/komentar.
    """
    line = raw_line.strip()
    if not line or line.startswith("%") or line.startswith("#"):
        return None

    # Format 1: Comma-delimited
    if "," in line:
        parts = [part.strip() for part in line.split(",") if part.strip()]
        if len(parts) >= 4:
            try:
                ts = parts[0]
                x = float(parts[1])
                y = float(parts[2])
                z = float(parts[3])
                q = int(parts[4]) if len(parts) >= 5 and parts[4].isdigit() else 1
                ns = int(parts[5]) if len(parts) >= 6 and parts[5].isdigit() else 0
                return ts, x, y, z, q, ns
            except ValueError:
                pass

    # Format 2: Whitespace-delimited (Standard RTKLIB solution)
    parts = line.split()
    if len(parts) >= 5:
        try:
            ts = f"{parts[0]} {parts[1]}"
            x = float(parts[2])
            y = float(parts[3])
            z = float(parts[4])
            q = int(parts[5]) if len(parts) >= 6 and parts[5].isdigit() else 1
            ns = int(parts[6]) if len(parts) >= 7 and parts[6].isdigit() else 0
            return ts, x, y, z, q, ns
        except ValueError:
            pass

    return None


def calculate_euclidean_distance(
    p1: Tuple[float, float, float],
    p2: Tuple[float, float, float]
) -> float:
    """Menghitung jarak spasial Euclidean 3D antara dua titik koordinat."""
    return math.sqrt(
        (p1[0] - p2[0]) ** 2 +
        (p1[1] - p2[1]) ** 2 +
        (p1[2] - p2[2]) ** 2
    )


# ==============================================================================
# MESIN MONITOR PERGESERAN DENGAN STRICT QUALITY GATE
# ==============================================================================
class GNSSDisplacementMonitor:
    """
    Mengelola kalibrasi baseline 60 sampel FIXED dan deteksi pergeseran real-time.
    Memastikan data FLOAT ditahan agar tidak memicu alarm palsu.
    """
    def __init__(
        self,
        baseline_samples: int = BASELINE_SAMPLE_COUNT,
        threshold_m: float = DISPLACEMENT_THRESHOLD_M,
        require_fixed: bool = REQUIRE_FIXED_ONLY,
        hazard_duration_sec: float = HAZARD_DURATION_SEC,
        on_alert: Optional[Callable[[float, str, Tuple[float, float, float], Tuple[float, float, float], float], Coroutine[Any, Any, None]]] = None,
        on_telemetry: Optional[Callable[[float, str, Tuple[float, float, float], Tuple[float, float, float]], Coroutine[Any, Any, None]]] = None,
        on_calibrating: Optional[Callable[[int, int], Coroutine[Any, Any, None]]] = None,
        on_quality_update: Optional[Callable[[int, str, int, bool], Coroutine[Any, Any, None]]] = None,
        on_baseline_updated: Optional[Callable[[Tuple[float, float, float], float, str], Coroutine[Any, Any, None]]] = None,
    ):
        self.baseline_samples_needed = baseline_samples
        self.threshold_m = threshold_m
        self.require_fixed = require_fixed
        self.hazard_duration_sec = hazard_duration_sec
        self.on_alert = on_alert
        self.on_telemetry = on_telemetry
        self.on_calibrating = on_calibrating
        self.on_quality_update = on_quality_update
        self.on_baseline_updated = on_baseline_updated

        self.calibration_buffer: List[Tuple[float, float, float]] = []
        self.baseline: Optional[Tuple[float, float, float]] = None
        self.last_pos: Optional[Tuple[float, float, float]] = None
        self._last_logged_q: Optional[int] = None
        self.is_in_hazard: bool = False

        # Variabel Auto-Rebaseline 1 Menit
        self.hazard_start_time: Optional[float] = None
        self.recent_settle_buffer: List[Tuple[float, float, float]] = []

    @property
    def is_calibrated(self) -> bool:
        return self.baseline is not None

    def reset_baseline(self) -> None:
        self.calibration_buffer.clear()
        self.baseline = None
        self.last_pos = None
        self.is_in_hazard = False
        self.hazard_start_time = None
        self.recent_settle_buffer.clear()
        print(f"🔄 [Baseline Reset] Memulai kalibrasi ulang {self.baseline_samples_needed} sampel FIXED konsisten...")

    async def process_reading(
        self,
        timestamp: str,
        x: float,
        y: float,
        z: float,
        q_flag: int = 1,
        num_sats: int = 0
    ) -> None:
        q_name = Q_NAME_MAP.get(q_flag, f"UNKNOWN({q_flag})")
        is_fixed = (q_flag == 1)

        # 1. Update status kualitas sinyal ke Frontend via WebSocket
        if self.on_quality_update:
            await self.on_quality_update(q_flag, q_name, num_sats, is_fixed)

        current_pos = (x, y, z)

        # 2. FASE KALIBRASI BASELINE: Harus FIXED konsisten sebanyak 60 sampel
        if not self.is_calibrated:
            if not is_fixed:
                # Terhalang FLOAT / status non-FIX saat kalibrasi berjalan
                if len(self.calibration_buffer) > 0:
                    prev_count = len(self.calibration_buffer)
                    self.calibration_buffer.clear()
                    print(
                        f"⚠️ [Kalibrasi Terhalang {q_name}] Sinyal bukan FIXED (terkumpul {prev_count}/{self.baseline_samples_needed}). "
                        f"Kalibrasi diulang dari awal [1/{self.baseline_samples_needed}]!"
                    )
                    if self.on_calibrating:
                        await self.on_calibrating(0, self.baseline_samples_needed)
                else:
                    if self._last_logged_q != q_flag:
                        print(f"⏳ Menunggu sinyal RTK FIXED untuk memulai kalibrasi {self.baseline_samples_needed} sampel (saat ini: {q_name}, Sat: {num_sats})...")
                        self._last_logged_q = q_flag
                return

            # Sinyal FIXED (Q=1) -> Masukkan ke buffer kalibrasi
            self.calibration_buffer.append(current_pos)
            current_count = len(self.calibration_buffer)
            self._last_logged_q = q_flag

            print(f"Calibrating baseline... [{current_count}/{self.baseline_samples_needed}] (FIXED)")

            if self.on_calibrating:
                await self.on_calibrating(current_count, self.baseline_samples_needed)

            if current_count >= self.baseline_samples_needed:
                avg_x = sum(p[0] for p in self.calibration_buffer) / current_count
                avg_y = sum(p[1] for p in self.calibration_buffer) / current_count
                avg_z = sum(p[2] for p in self.calibration_buffer) / current_count
                self.baseline = (avg_x, avg_y, avg_z)

                print(
                    f"✅ Baseline established ({self.baseline_samples_needed} sampel FIXED konsisten): "
                    f"X={avg_x:.4f}m, Y={avg_y:.4f}m, Z={avg_z:.4f}m"
                )
            return

        # 3. FASE SETELAH BASELINE TERBENTUK (MONITORING AKTIF):
        # Jika bukan FIXED, tahan data! Hanya data FIXED yang diproses untuk pergeseran & peringatan.
        if self.require_fixed and not is_fixed:
            if self._last_logged_q != q_flag:
                print(f"⏳ [FILTER RTK] Baseline terkunci. Status {q_name} (Q={q_flag}, Sat: {num_sats}) -> Data ditahan (hanya FIXED yang mengeluarkan data).")
                self._last_logged_q = q_flag
            return

        # Log transisi status FIX di console
        if self._last_logged_q != q_flag:
            print(f"[✅ FIXED] Status RTK: FIXED (Q=1, Sat: {num_sats}). Data pergeseran aktif diproses.")
            self._last_logged_q = q_flag

        # 4. Deteksi Pergeseran Spasial Real-Time (Data FIXED)
        delta_m = calculate_euclidean_distance(current_pos, self.baseline)
        delta_cm = delta_m * 100.0

        if delta_m > self.threshold_m:
            now_t = time.time()
            if self.hazard_start_time is None:
                self.hazard_start_time = now_t
                self.recent_settle_buffer.clear()
                self.is_in_hazard = True
                print(f"\n🚨 [STATUS BAHAYA] Pergeseran tanah melampaui {DISPLACEMENT_THRESHOLD_CM:.1f} cm ({delta_cm:.1f} cm)!")
                print(f"⏳ Peringatan darurat aktif ~1 menit ({self.hazard_duration_sec:.0f}s). Baseline akan diperbarui ke posisi baru jika getaran mereda.")

            self.recent_settle_buffer.append(current_pos)
            if len(self.recent_settle_buffer) > 15:
                self.recent_settle_buffer.pop(0)

            elapsed_hazard = now_t - self.hazard_start_time
            remaining_hazard = max(0.0, self.hazard_duration_sec - elapsed_hazard)

            # Jika sudah berjalan 1 menit (>= 60 detik) pasca-pergeseran
            if elapsed_hazard >= self.hazard_duration_sec:
                buf_len = len(self.recent_settle_buffer)
                avg_x = sum(p[0] for p in self.recent_settle_buffer) / buf_len
                avg_y = sum(p[1] for p in self.recent_settle_buffer) / buf_len
                avg_z = sum(p[2] for p in self.recent_settle_buffer) / buf_len
                candidate_pos = (avg_x, avg_y, avg_z)

                max_jitter = max(calculate_euclidean_distance(p, candidate_pos) for p in self.recent_settle_buffer)

                # Jika tanah sudah tidak bergeser lagi (stabil / jitter < 5 cm)
                if max_jitter < SETTLE_MAX_JITTER_M or buf_len >= 8:
                    old_baseline = self.baseline
                    permanent_shift_m = calculate_euclidean_distance(candidate_pos, old_baseline)
                    permanent_shift_cm = permanent_shift_m * 100.0

                    self.baseline = candidate_pos
                    self.is_in_hazard = False
                    self.hazard_start_time = None
                    self.recent_settle_buffer.clear()

                    print(f"\n" + "="*75)
                    print(f"🔄 [AUTO-REBASELINE 1 MENIT] Peringatan 60 detik selesai.")
                    print(f"   📍 Getaran mereda & tanah telah stabil pada posisi baru.")
                    print(f"   📏 Pergeseran Permanen Co-Seismic: {permanent_shift_cm:.1f} cm")
                    print(f"   🧭 Baseline Baru Terkunci: X={avg_x:.4f}m, Y={avg_y:.4f}m, Z={avg_z:.4f}m")
                    print(f"   🛡️ Status kembali AMAN. Memulai pemantauan dari baseline baru ini.")
                    print("="*75 + "\n")

                    if self.on_baseline_updated:
                        await self.on_baseline_updated(self.baseline, permanent_shift_cm, timestamp)

                    if self.on_telemetry:
                        await self.on_telemetry(0.0, timestamp, current_pos, self.baseline)
                    return

            # Masih dalam rentang 1 menit peringatan aktif
            print(f"🚨 BENCANA: Pergeseran Tanah Terdeteksi ({delta_cm:.1f} cm)! Sisa durasi alert: {remaining_hazard:.0f}s")
            print(f"   📍 Koordinat : X={current_pos[0]:.3f}m, Y={current_pos[1]:.3f}m, Z={current_pos[2]:.3f}m")

            if self.on_alert:
                await self.on_alert(delta_m, timestamp, current_pos, self.baseline, remaining_hazard)
        else:
            if self.is_in_hazard:
                self.is_in_hazard = False
                self.hazard_start_time = None
                self.recent_settle_buffer.clear()
                print(f"\n✅ [STATUS TRANSISI] Getaran mereda ({delta_cm:.1f} cm <= {DISPLACEMENT_THRESHOLD_CM:.1f} cm) -> Status AMAN & menonaktifkan alarm.")

            print(f"[✅ AMAN] {timestamp} | Pergeseran: {delta_cm:4.1f} cm | Toleransi: 1-3 cm | RTK FIXED ({num_sats} Sat)")

            if self.on_telemetry:
                await self.on_telemetry(delta_m, timestamp, current_pos, self.baseline)


# ==============================================================================
# ASYNC TCP STREAM CLIENT WITH AUTO-RECONNECT
# ==============================================================================
class AsyncGNSSSocketClient:
    """Menghubungkan ke RTKLIB str2str TCP broadcast stream secara asinkron."""

    def __init__(
        self,
        host: str = TCP_IP,
        port: int = TCP_PORT,
        reconnect_delay: float = RECONNECT_DELAY_SEC,
        timeout: float = SOCKET_TIMEOUT_SEC,
        monitor: Optional[GNSSDisplacementMonitor] = None,
    ):
        self.host = host
        self.port = port
        self.reconnect_delay = reconnect_delay
        self.timeout = timeout
        self.monitor = monitor or GNSSDisplacementMonitor()
        self._is_running = False

    def stop(self) -> None:
        self._is_running = False

    async def run(self) -> None:
        self._is_running = True
        print(f"📡 GNSS Stream Target: {self.host}:{self.port}")

        while self._is_running:
            reader: Optional[asyncio.StreamReader] = None
            writer: Optional[asyncio.StreamWriter] = None
            try:
                print(f"Menghubungkan ke GNSS Stream di {self.host}:{self.port}...")
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.timeout
                )
                print(f"✅ Terhubung ke GNSS Stream di {self.host}:{self.port}. Mendengarkan data...")

                while self._is_running:
                    line_bytes = await asyncio.wait_for(
                        reader.readline(),
                        timeout=self.timeout
                    )
                    if not line_bytes:
                        raise ConnectionResetError("Remote GNSS server closed the connection.")

                    line_str = line_bytes.decode("utf-8", errors="replace")
                    parsed = parse_gnss_line(line_str)
                    if parsed:
                        ts, x, y, z, q, ns = parsed
                        await self.monitor.process_reading(ts, x, y, z, q, ns)

            except (asyncio.TimeoutError, TimeoutError):
                print(f"⚠️ [TCP] Timeout: Tidak ada data diterima selama {self.timeout} detik.")
            except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError, OSError) as e:
                print(f"⚠️ [TCP] Connection error ({type(e).__name__}): {e}")
            except Exception as e:
                print(f"⚠️ [TCP] Kesalahan stream: {e}")
            finally:
                if writer:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass

            if self._is_running:
                print(f"🔄 Reconnecting ke GNSS stream dalam {self.reconnect_delay} detik...")
                await asyncio.sleep(self.reconnect_delay)


# ==============================================================================
# WEBSOCKET SERVER COROUTINE
# ==============================================================================
async def start_websocket_server(ws_manager: WebSocketManager, monitor: Optional[GNSSDisplacementMonitor] = None, host: str = WS_HOST, port: int = WS_PORT) -> None:
    """Menjalankan WebSocket Server asinkron."""
    async def client_handler(websocket: Any, *args: Any, **kwargs: Any) -> None:
        await ws_manager.register(websocket)
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    action = data.get("action")
                    if action == "PING":
                        await websocket.send(json.dumps({"type": "PONG", "timestamp": time.time()}))
                    elif action == "RESET_BASELINE" and monitor:
                        monitor.reset_baseline()
                except Exception:
                    pass
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await ws_manager.unregister(websocket)

    print(f"🌐 WebSocket Server aktif di ws://localhost:{port} (bound to {host}:{port})")
    async with websockets.serve(client_handler, host, port):
        await asyncio.Future()


# ==============================================================================
# MAIN ASYNC RUNNER
# ==============================================================================
async def main() -> None:
    ws_manager = WebSocketManager()
    wa_tracker = AlertCooldownTracker(cooldown_sec=WA_COOLDOWN_SEC)

    # Callback ketika pergeseran > ambang batas (10 cm)
    async def alert_callback(
        delta_m: float,
        timestamp: str,
        current_pos: Tuple[float, float, float],
        baseline_pos: Tuple[float, float, float],
        remaining_sec: float = 60.0
    ) -> None:
        delta_cm = round(delta_m * 100.0, 1)
        payload = {
            "type": "ALERT",
            "status": "HAZARD",
            "delta_cm": delta_cm,
            "delta_m": round(delta_m, 4),
            "remaining_sec": round(remaining_sec, 1),
            "hazard_duration_sec": HAZARD_DURATION_SEC,
            "timestamp": timestamp,
            "coords": {
                "x": round(current_pos[0], 3),
                "y": round(current_pos[1], 3),
                "z": round(current_pos[2], 3)
            },
            "baseline": {
                "x": round(baseline_pos[0], 3),
                "y": round(baseline_pos[1], 3),
                "z": round(baseline_pos[2], 3)
            }
        }
        await ws_manager.broadcast(payload)

        # WhatsApp alert di background (non-blocking)
        if wa_tracker.can_send():
            wa_tracker.mark_sent()
            asyncio.create_task(send_whatsapp_alert(delta_cm, timestamp, current_pos))
        else:
            remaining = wa_tracker.time_remaining()
            print(f"⏳ [WhatsApp] Cooldown aktif ({remaining:.1f}s tersisa). Pesan WA ditahan.")

    # Callback saat baseline otomatis diperbarui ke posisi baru setelah 1 menit gempa mereda
    async def baseline_updated_callback(
        new_baseline: Tuple[float, float, float],
        permanent_shift_cm: float,
        timestamp: str
    ) -> None:
        payload = {
            "type": "BASELINE_UPDATED",
            "status": "SAFE",
            "delta_cm": 0.0,
            "delta_m": 0.0,
            "permanent_shift_cm": round(permanent_shift_cm, 1),
            "timestamp": timestamp,
            "coords": {
                "x": round(new_baseline[0], 3),
                "y": round(new_baseline[1], 3),
                "z": round(new_baseline[2], 3)
            },
            "baseline": {
                "x": round(new_baseline[0], 3),
                "y": round(new_baseline[1], 3),
                "z": round(new_baseline[2], 3)
            }
        }
        await ws_manager.broadcast(payload)

    # Callback saat kondisi normal (<= 10 cm)
    async def telemetry_callback(
        delta_m: float,
        timestamp: str,
        current_pos: Tuple[float, float, float],
        baseline_pos: Tuple[float, float, float]
    ) -> None:
        delta_cm = round(delta_m * 100.0, 1)
        payload = {
            "type": "TELEMETRY",
            "status": "SAFE",
            "delta_cm": delta_cm,
            "delta_m": round(delta_m, 4),
            "timestamp": timestamp,
            "coords": {
                "x": round(current_pos[0], 3),
                "y": round(current_pos[1], 3),
                "z": round(current_pos[2], 3)
            },
            "baseline": {
                "x": round(baseline_pos[0], 3),
                "y": round(baseline_pos[1], 3),
                "z": round(baseline_pos[2], 3)
            }
        }
        await ws_manager.broadcast(payload)

    # Callback progress kalibrasi baseline 60 sampel
    async def calibrating_callback(current: int, total: int) -> None:
        payload = {
            "type": "CALIBRATING",
            "current": current,
            "total": total
        }
        await ws_manager.broadcast(payload)

    # Callback pembaruan kualitas solusi RTK
    async def quality_callback(q_code: int, q_name: str, satellites: int, is_fixed: bool) -> None:
        payload = {
            "type": "QUALITY_UPDATE",
            "quality": q_name,
            "q_code": q_code,
            "satellites": satellites,
            "is_fixed": is_fixed,
            "rtx_locked": is_fixed
        }
        await ws_manager.broadcast(payload)

    # Inisialisasi Monitor Engine
    monitor = GNSSDisplacementMonitor(
        baseline_samples=BASELINE_SAMPLE_COUNT,
        threshold_m=DISPLACEMENT_THRESHOLD_M,
        require_fixed=REQUIRE_FIXED_ONLY,
        hazard_duration_sec=HAZARD_DURATION_SEC,
        on_alert=alert_callback,
        on_telemetry=telemetry_callback,
        on_calibrating=calibrating_callback,
        on_quality_update=quality_callback,
        on_baseline_updated=baseline_updated_callback,
    )

    # Inisialisasi TCP Client ke sensor RTKLIB
    tcp_client = AsyncGNSSSocketClient(
        host=TCP_IP,
        port=TCP_PORT,
        reconnect_delay=RECONNECT_DELAY_SEC,
        timeout=SOCKET_TIMEOUT_SEC,
        monitor=monitor,
    )

    # Jalankan WebSocket server dan TCP Client secara bersamaan
    try:
        await asyncio.gather(
            start_websocket_server(ws_manager, monitor, WS_HOST, WS_PORT),
            tcp_client.run(),
        )
    except (asyncio.CancelledError, KeyboardInterrupt):
        print("\n🛑 Menghentikan layanan pemantau GNSS...")
    finally:
        tcp_client.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 GeoAlert Backend berhenti.")
