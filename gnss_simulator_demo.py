#!/usr/bin/env python3
"""
GeoAlert - GNSS Seismology & Real-Time Earthquake Simulation Engine (DEMO MODE)
-------------------------------------------------------------------------------
Simulator Mandiri Khusus Demonstrasi & Presentasi Lomba (Bumirekha 2026):
1. Menjalankan WebSocket Server di ws://localhost:8765 secara mandiri (tanpa alat GNSS fisik).
2. Menghasilkan alur simulasi real-time looping tanpa henti (Infinite Scenario Loop):
   - Inisialisasi & Kalibrasi Baseline 60 sampel (RTK FIXED).
   - Kondisi Aman: Pergeseran normal 1.0 cm - 2.5 cm (< 10 cm ambang batas).
   - Efek Realistis: Dinamika satelit 31-34, koreksi RTX sesekali sinkronisasi ulang.
   - BENCANA GEMPA BUMI: Tiba-tiba pergeseran melonjak drastis hingga 25-30 cm (> 10 cm)!
     -> Sirine audio frontend MENYALA OTOMATIS & UI Merah Berkedip!
     -> Notifikasi WhatsApp BPBD disimulasikan terkirim.
   - Gempa Mereda: Pergeseran berangsur turun kembali di bawah 10 cm.
     -> Sirine audio frontend MATI OTOMATIS & UI kembali Hijau (AMAN)!
   - Siklus berulang terus menerus secara otomatis tanpa perlu klik apapun!

Author: Expert IoT & Full-Stack Seismology Engineer
"""

import asyncio
import datetime
import json
import math
import random
import sys
import time
from typing import Any, Dict, Set

# Cek pustaka websockets
try:
    import websockets
except ImportError:
    print("\n❌ Error: Paket 'websockets' belum terinstall.")
    print("👉 Silakan jalankan: pip install websockets\n")
    sys.exit(1)

# Konfigurasi Server
WS_HOST = "0.0.0.0"
WS_PORT = 8765
THRESHOLD_CM = 10.0  # Batas ambang gempa: 10 Centimeter

# Posisi Baseline Geodesi (Stasiun Bandung Utara GALT-BDG-01)
BASE_X = 2233.804
BASE_Y = -8077.142
BASE_Z = -610.071


class WebSocketBroadcaster:
    """Mengelola koneksi client frontend dan mem-broadcast pesan JSON."""

    def __init__(self) -> None:
        self.clients: Set[Any] = set()

    async def register(self, ws: Any) -> None:
        self.clients.add(ws)
        print(f"🔌 [WebSocket] Frontend Dashboard terhubung! Total klien: {len(self.clients)}")

    async def unregister(self, ws: Any) -> None:
        self.clients.discard(ws)
        print(f"🔌 [WebSocket] Frontend terputus. Sisa klien: {len(self.clients)}")

    async def broadcast(self, payload: Dict[str, Any]) -> None:
        if not self.clients:
            return
        message = json.dumps(payload)
        stale_clients = set()
        for client in self.clients:
            try:
                await client.send(message)
            except Exception:
                stale_clients.add(client)
        for client in stale_clients:
            self.clients.discard(client)


async def run_scenario_loop(broadcaster: WebSocketBroadcaster) -> None:
    """Loop skenario demonstrasi otomatis yang terus berulang selamanya."""
    loop_count = 1

    print("\n" + "=" * 70)
    print("🌍 GEOALERT - GNSS REAL-TIME DISPLACEMENT SIMULATOR (DEMO MODE)")
    print("=" * 70)
    print(f"📡 WebSocket Stream: ws://localhost:{WS_PORT}")
    print(f"📏 Ambang Bahaya   : {THRESHOLD_CM:.1f} cm (Pergeseran > 10 cm = Bencana Gempa)")
    print("🔄 Mode Demonstrasi: Otomatis Looping Kontinu (Tanpa Perlu Klik)")
    print("=" * 70 + "\n")

    while True:
        timestamp_now = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"\n▶️ [SIMULASI PUTARAN #{loop_count}] Memulai skenario siklus baru pada {timestamp_now}...")

        # ----------------------------------------------------------------------
        # FASE 1: KALIBRASI BASELINE 60 SAMPEL (Cepat & Elegan ~5 detik)
        # ----------------------------------------------------------------------
        print("📡 Menghubungkan ke Sensor GNSS RTKNAVI (100.122.238.102:9000)...")
        await broadcaster.broadcast({
            "type": "SYSTEM_UPDATE",
            "tailscale_status": "Terhubung ke Sensor / Tailscale",
            "rtx_status": "Mencari Fix...",
            "rtx_locked": False,
        })
        await asyncio.sleep(1.0)

        # Update Kualitas: RTK FIXED 31 Satelit
        await broadcaster.broadcast({
            "type": "QUALITY_UPDATE",
            "quality": "FIXED",
            "q_code": 1,
            "satellites": 31,
            "is_fixed": True,
            "rtx_locked": True,
        })

        # Emulasi progress kalibrasi 1..60
        for count in [12, 28, 45, 60]:
            print(f"Calibrating baseline... [{count}/60] (FIXED)")
            await broadcaster.broadcast({
                "type": "CALIBRATING",
                "current": count,
                "total": 60,
            })
            await asyncio.sleep(0.8)

        print(f"✅ Baseline established (60 sampel FIXED): X={BASE_X:.3f}m, Y={BASE_Y:.3f}m, Z={BASE_Z:.3f}m")

        # ----------------------------------------------------------------------
        # FASE 2: KONDISI NORMAL / AMAN (16 Detik)
        # Pergeseran mikrodeformasi alami 0.8 cm - 2.5 cm (< 10 cm)
        # ----------------------------------------------------------------------
        print("\n🛡️ [FASE AMAN] Memasuki pemantauan normal (Pergeseran aman < 10 cm)...")
        for step in range(16):
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            # Noise osilasi mikrodeformasi alami antara 0.9 cm hingga 2.4 cm
            wave = 1.3 + 0.6 * math.sin(step * 0.5) + (random.uniform(-0.25, 0.25))
            delta_cm = round(max(0.6, wave), 1)
            delta_m = round(delta_cm / 100.0, 4)

            # Koordinat berfluktuasi tipis dalam milimeter
            cur_x = round(BASE_X + random.uniform(-0.012, 0.012), 3)
            cur_y = round(BASE_Y + random.uniform(-0.012, 0.012), 3)
            cur_z = round(BASE_Z + random.uniform(-0.012, 0.012), 3)

            sat_count = random.choice([31, 32, 33])

            payload = {
                "type": "TELEMETRY",
                "status": "SAFE",
                "delta_cm": delta_cm,
                "delta_m": delta_m,
                "timestamp": ts,
                "coords": {"x": cur_x, "y": cur_y, "z": cur_z},
                "baseline": {"x": BASE_X, "y": BASE_Y, "z": BASE_Z},
                "satellites": sat_count,
                "quality": "FIXED",
                "is_fixed": True,
                "rtx_locked": True,
            }
            await broadcaster.broadcast(payload)
            print(f"   [🟢 AMAN] {ts} | Pergeseran: {delta_cm:4.1f} cm | Toleransi: 1-3 cm | RTK FIXED ({sat_count} Sat)")
            await asyncio.sleep(1.0)

        # ----------------------------------------------------------------------
        # FASE 3: GELOMBANG GEMPA BUMI TERJADI! (> 10 CM!) (12 Detik)
        # Deformasi melonjak drastis hingga 28 cm -> SIRINE OTOMATIS MENYALA!
        # ----------------------------------------------------------------------
        print("\n🚨🚨🚨 [FASE GEMPA BUMI] GELOMBANG SEISMIK BESAR TERDETEKSI (> 10 CM)! 🚨🚨🚨")
        print("📢 SIRINE FRONTEND AKAN MENYALA OTOMATIS & DASHBOARD BERKEDIP MERAH!")

        # Profil gelombang deformasi seismik gempa (dalam centimeter)
        earthquake_profile = [
            11.4, 15.8, 22.4, 28.6, 29.8, 27.2, 23.5, 19.1, 15.4, 12.1
        ]

        for idx, delta_cm in enumerate(earthquake_profile):
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            delta_m = round(delta_cm / 100.0, 4)

            # Pergeseran fisik tanah signifikan
            shift = (delta_cm / 100.0) * 0.707
            cur_x = round(BASE_X + shift + random.uniform(-0.01, 0.01), 3)
            cur_y = round(BASE_Y - shift + random.uniform(-0.01, 0.01), 3)
            cur_z = round(BASE_Z + (shift * 0.3), 3)

            payload = {
                "type": "ALERT",
                "status": "HAZARD",
                "delta_cm": delta_cm,
                "delta_m": delta_m,
                "timestamp": ts,
                "coords": {"x": cur_x, "y": cur_y, "z": cur_z},
                "baseline": {"x": BASE_X, "y": BASE_Y, "z": BASE_Z},
                "satellites": 31,
                "quality": "FIXED",
                "is_fixed": True,
                "rtx_locked": True,
            }
            await broadcaster.broadcast(payload)

            if idx == 0:
                print(f"🚨 [ALARM BENCANA AKTIF] {ts} | AMBANG BATAS 10 CM TERLAMPAUI: {delta_cm} cm!")
                print("📲 [Simulasi WA BPBD] Mengirim pesan darurat ke WhatsApp Stakeholder (+6282119709784)...")
            else:
                print(f"🚨 [BAHAYA GEMPA] {ts} | Besar Pergeseran: {delta_cm:4.1f} cm | Sirene Aktif")

            await asyncio.sleep(1.0)

        # ----------------------------------------------------------------------
        # FASE 4: GEMPA MEREDA & PEMULIHAN OTOMATIS (6 Detik)
        # Deformasi kembali turun < 10 cm -> SIRINE OTOMATIS MATI!
        # ----------------------------------------------------------------------
        print("\n✅ [FASE PEMULIHAN] Getaran gempa mereda di bawah 10 cm...")
        print("🔇 SIRINE FRONTEND AKAN MATI OTOMATIS & DASHBOARD KEMBALI HIJAU (AMAN)!")

        subsiding_profile = [8.4, 5.2, 3.1, 1.8, 1.4, 1.2]
        for delta_cm in subsiding_profile:
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            delta_m = round(delta_cm / 100.0, 4)
            cur_x = round(BASE_X + random.uniform(-0.015, 0.015), 3)
            cur_y = round(BASE_Y + random.uniform(-0.015, 0.015), 3)
            cur_z = round(BASE_Z + random.uniform(-0.015, 0.015), 3)

            payload = {
                "type": "TELEMETRY",
                "status": "SAFE",
                "delta_cm": delta_cm,
                "delta_m": delta_m,
                "timestamp": ts,
                "coords": {"x": cur_x, "y": cur_y, "z": cur_z},
                "baseline": {"x": BASE_X, "y": BASE_Y, "z": BASE_Z},
                "satellites": 32,
                "quality": "FIXED",
                "is_fixed": True,
                "rtx_locked": True,
            }
            await broadcaster.broadcast(payload)
            print(f"   [🟢 REDA] {ts} | Pergeseran: {delta_cm:4.1f} cm | Kondisi Kembali Aman | Sirene Mati")
            await asyncio.sleep(1.0)

        # ----------------------------------------------------------------------
        # FASE 5: VARIASI REALISTIS (SESEKALI DINAMIKA RECONNECT SENSOR) (3 Detik)
        # ----------------------------------------------------------------------
        print("🛰️ [Dinamika Sensor] Simulasi verifikasi sinkronisasi RTX...")
        await broadcaster.broadcast({
            "type": "QUALITY_UPDATE",
            "quality": "FLOAT",
            "q_code": 2,
            "satellites": 27,
            "is_fixed": False,
            "rtx_locked": False,
        })
        await asyncio.sleep(1.5)

        await broadcaster.broadcast({
            "type": "QUALITY_UPDATE",
            "quality": "FIXED",
            "q_code": 1,
            "satellites": 32,
            "is_fixed": True,
            "rtx_locked": True,
        })
        await asyncio.sleep(1.5)

        loop_count += 1
        print(f"🔄 Putaran selesai! Mengulang skenario berikutnya...")


async def main() -> None:
    broadcaster = WebSocketBroadcaster()

    async def client_handler(websocket: Any, *args: Any, **kwargs: Any) -> None:
        await broadcaster.register(websocket)
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    action = data.get("action")
                    if action == "PING":
                        await websocket.send(json.dumps({"type": "PONG", "timestamp": time.time()}))
                    elif action == "RESET_BASELINE":
                        print("🔄 [Command] Permintaan reset baseline manual diterima dari Frontend.")
                except Exception:
                    pass
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await broadcaster.unregister(websocket)

    # Jalankan WebSocket server dan loop skenario secara bersamaan
    server = await websockets.serve(client_handler, WS_HOST, WS_PORT)
    print(f"🌐 WebSocket Server aktif di ws://localhost:{WS_PORT}")

    try:
        await asyncio.gather(
            run_scenario_loop(broadcaster),
        )
    except (asyncio.CancelledError, KeyboardInterrupt):
        print("\n🛑 Simulator Demo dihentikan.")
    finally:
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 GeoAlert Simulator Demo selesai.")

