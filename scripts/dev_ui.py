#!/usr/bin/env python3
"""Serve the viewer locally the way nginx does on the server.

On the server, nginx serves ./ui as static files and proxies /api/ to the
FastAPI container and /media/ to its thumbnail mount. The PC has no nginx, so
this script does the same three jobs with the standard library only:

  /api/<path>    -> <api>/<path>       (the /api prefix is stripped, as nginx does)
  /media/<path>  -> <media>/<path>     (thumbnails; by default the server's copy)
  everything else -> files under ./ui, served with caching disabled

Typical uses, from the repo root:

  # UI work only, against the live server API
  python scripts/dev_ui.py --api http://192.168.1.164:8000

  # UI + API work: run the API locally first (see ui/README.md), then
  python scripts/dev_ui.py

Then open http://localhost:8080/.
"""

import argparse
import http.server
import os
import sys
import urllib.error
import urllib.request
from functools import partial

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-encoding",
}


class Handler(http.server.SimpleHTTPRequestHandler):
    api_base = "http://127.0.0.1:8000"
    media_base = "http://192.168.1.164:8000/media"

    def do_GET(self):
        # The page asks for stills as /api/media/...; a local API has no media
        # folder, so those go to the media host too.
        if self.path.startswith("/api/media/"):
            return self._proxy(self.media_base + self.path[len("/api/media"):])
        if self.path.startswith("/api/"):
            return self._proxy(self.api_base + self.path[len("/api"):])
        if self.path.startswith("/media/"):
            return self._proxy(self.media_base + self.path[len("/media"):])
        return super().do_GET()

    def _proxy(self, url):
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                self._relay(resp.status, resp.headers, resp.read())
        except urllib.error.HTTPError as e:
            self._relay(e.code, e.headers, e.read())
        except urllib.error.URLError as e:
            body = f"dev_ui: cannot reach {url}: {e.reason}".encode()
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def _relay(self, status, headers, body):
        self.send_response(status)
        for k, v in headers.items():
            if k.lower() in HOP_BY_HOP or k.lower() == "content-length":
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self):
        # Static files change constantly while developing; never cache them.
        if not self.path.startswith(("/api/", "/media/")):
            self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (self.log_date_time_string(), fmt % args))


def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--root", default=os.path.join(repo_root, "ui"),
                   help="folder of static files (default: ./ui)")
    p.add_argument("--api", default=Handler.api_base,
                   help="where /api/ requests go (default: a local uvicorn)")
    p.add_argument("--media", default=Handler.media_base,
                   help="where /media/ requests go (default: the server's thumbnails)")
    a = p.parse_args()

    Handler.api_base = a.api.rstrip("/")
    Handler.media_base = a.media.rstrip("/")
    handler = partial(Handler, directory=a.root)
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", a.port), handler)
    print(f"serving {a.root} on http://localhost:{a.port}/")
    print(f"  /api/   -> {Handler.api_base}/")
    print(f"  /media/ -> {Handler.media_base}/")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
