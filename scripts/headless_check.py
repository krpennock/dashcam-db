#!/usr/bin/env python3
"""Drive a headless Edge/Chrome through the DevTools protocol, standard library only.

A plain --screenshot run of a headless browser stops drawing frames as soon as
the page's network goes quiet, so a map whose data arrived last never shows
it. This script keeps the browser alive, waits real time, and can ask the page
questions (run JavaScript) and take screenshots whenever asked.

Steps run in the order given:

  --do wait=MS            sleep that many milliseconds (real time)
  --do eval=EXPRESSION    evaluate JS in the page (awaits a promise), print result
  --do shot=PATH          save a PNG screenshot
  --do click=SELECTOR     click the first element matching a CSS selector

Console messages and uncaught exceptions from the page are printed at the end
(browser-extension noise is left out because a fresh profile has none).

Example:

  python scripts/headless_check.py "http://localhost:8080/index.html?session=..." \
      --do wait=6000 \
      --do "eval=__dashcam.map.queryRenderedFeatures({layers:['track-line']}).length" \
      --do shot=out.png
"""

import argparse
import base64
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request

BROWSERS = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]


class WebSocket:
    """The little of RFC 6455 a local DevTools connection needs."""

    def __init__(self, url):
        assert url.startswith("ws://")
        hostport, _, path = url[5:].partition("/")
        host, _, port = hostport.partition(":")
        self.sock = socket.create_connection((host, int(port or 80)))
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f"GET /{path} HTTP/1.1\r\nHost: {hostport}\r\nUpgrade: websocket\r\n"
               f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n")
        self.sock.sendall(req.encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("websocket handshake failed")
            resp += chunk
        if b" 101 " not in resp.split(b"\r\n", 1)[0]:
            raise RuntimeError(f"websocket handshake refused: {resp[:200]!r}")
        self.buf = resp.split(b"\r\n\r\n", 1)[1]

    def _recv_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(max(65536, n - len(self.buf)))
            if not chunk:
                raise RuntimeError("websocket closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def send(self, text):
        payload = text.encode()
        head = bytes([0x81])
        n = len(payload)
        if n < 126:
            head += bytes([0x80 | n])
        elif n < 65536:
            head += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            head += bytes([0x80 | 127]) + struct.pack(">Q", n)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(head + mask + masked)

    def recv(self):
        """One complete text message (fragments are joined)."""
        message = b""
        while True:
            b0, b1 = self._recv_exact(2)
            fin, opcode = b0 & 0x80, b0 & 0x0F
            n = b1 & 0x7F
            if n == 126:
                n = struct.unpack(">H", self._recv_exact(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", self._recv_exact(8))[0]
            if b1 & 0x80:
                mask = self._recv_exact(4)
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(self._recv_exact(n)))
            else:
                data = self._recv_exact(n)
            if opcode == 0x8:
                raise RuntimeError("websocket closed by peer")
            if opcode == 0x9:  # ping
                self.sock.sendall(bytes([0x8A, 0x80]) + os.urandom(4))
                continue
            message += data
            if fin:
                return message.decode()


class CDP:
    def __init__(self, ws_url):
        self.ws = WebSocket(ws_url)
        self.next_id = 0
        self.events = []

    def call(self, method, **params):
        self.next_id += 1
        mid = self.next_id
        self.ws.send(json.dumps({"id": mid, "method": method, "params": params}))
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == mid:
                if "error" in msg:
                    raise RuntimeError(f"{method}: {msg['error']}")
                return msg.get("result", {})
            if "method" in msg:
                self.events.append(msg)

    def pump(self, seconds):
        """Collect events for a while."""
        self.ws.sock.settimeout(0.2)
        end = time.time() + seconds
        try:
            while time.time() < end:
                try:
                    msg = json.loads(self.ws.recv())
                except socket.timeout:
                    continue
                if "method" in msg:
                    self.events.append(msg)
        finally:
            self.ws.sock.settimeout(None)

    def eval(self, expression):
        r = self.call("Runtime.evaluate", expression=expression, awaitPromise=True, returnByValue=True)
        if "exceptionDetails" in r:
            ex = r["exceptionDetails"]
            text = ex.get("exception", {}).get("description") or ex.get("text")
            return f"EXCEPTION: {text}"
        return r.get("result", {}).get("value")


def find_browser():
    for p in BROWSERS:
        if os.path.exists(p):
            return p
    sys.exit("no Edge or Chrome found")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("url")
    ap.add_argument("--do", action="append", default=[], metavar="STEP",
                    help="wait=MS | eval=JS | shot=PATH | click=SELECTOR (repeatable, in order)")
    ap.add_argument("--size", default="1600,1000", help="window size, WIDTH,HEIGHT")
    ap.add_argument("--port", type=int, default=9333)
    a = ap.parse_args()

    profile = tempfile.mkdtemp(prefix="headless_check_")
    browser = find_browser()
    proc = subprocess.Popen([
        browser, "--headless=new", "--disable-gpu", "--no-first-run", "--disable-extensions",
        f"--remote-debugging-port={a.port}", f"--user-data-dir={profile}",
        f"--window-size={a.size}", "about:blank",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        targets = None
        for _ in range(100):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{a.port}/json/list", timeout=1) as r:
                    targets = json.load(r)
                if any(t.get("type") == "page" for t in targets):
                    break
            except Exception:
                pass
            time.sleep(0.1)
        page = next(t for t in targets if t.get("type") == "page")
        cdp = CDP(page["webSocketDebuggerUrl"])
        cdp.call("Page.enable")
        cdp.call("Runtime.enable")
        cdp.call("Log.enable")
        cdp.call("Page.navigate", url=a.url)
        cdp.pump(1.0)

        for step in a.do:
            kind, _, arg = step.partition("=")
            if kind == "wait":
                cdp.pump(int(arg) / 1000)
            elif kind == "eval":
                print(f"eval {arg}\n  -> {json.dumps(cdp.eval(arg))}")
            elif kind == "shot":
                data = cdp.call("Page.captureScreenshot", format="png")["data"]
                with open(arg, "wb") as f:
                    f.write(base64.b64decode(data))
                print(f"shot {arg}")
            elif kind == "click":
                print(f"click {arg} -> {cdp.eval(f'(e=>e?(e.click(),true):false)(document.querySelector({json.dumps(arg)}))')}")
            else:
                sys.exit(f"unknown step: {step}")

        print("--- console")
        for ev in cdp.events:
            m = ev["method"]
            p = ev.get("params", {})
            if m == "Runtime.consoleAPICalled":
                text = " ".join(str(x.get("value", x.get("description", ""))) for x in p.get("args", []))
                print(f"[{p.get('type')}] {text[:300]}")
            elif m == "Runtime.exceptionThrown":
                d = p.get("exceptionDetails", {})
                print(f"[exception] {d.get('exception', {}).get('description') or d.get('text')}")
            elif m == "Log.entryAdded":
                e = p.get("entry", {})
                print(f"[{e.get('level')}] {e.get('text', '')[:300]}  {e.get('url', '')}")
    finally:
        proc.kill()
        proc.wait()
        shutil.rmtree(profile, ignore_errors=True)


if __name__ == "__main__":
    main()
