import os
import msgpack
from flask import Flask, make_response, render_template_string, request
from markupsafe import Markup
from cachetools import LRUCache
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import mmap
from urllib.parse import quote
from pathlib import Path

app = Flask(__name__)

# Base directory to browse (adjust as needed)
BASE_DIR = os.environ.get("DATA_PATH", ".")

# LRU cache for directory listings
dir_cache = LRUCache(maxsize=128)
mapfile_cache = LRUCache(maxsize=16)
# Track which directories we've already set up inotify for
watched_dirs = set()

# Set up observer for file-system events
observer = Observer()
observer.daemon = True
observer.start()


class CacheEvictHandler(FileSystemEventHandler):
    def __init__(self, cache, key):
        super().__init__()
        self.cache = cache
        self.key = key

    def on_any_event(self, event):
        # Evict this path from cache on any change
        if self.key in self.cache:
            self.cache.pop(self.key, None)


# HTML template with HTMX and scrollable panes
BASE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>WebDataset Index Browser</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet"/>
  <link href="https://fonts.cdnfonts.com/css/anonymous-pro" rel="stylesheet">
  <style>
    #file-tree, #table-container {
      max-height: calc(100vh - 150px);
      overflow-y: auto;
    }
    #file-tree.htmx-request, #table-container.htmx-request {
      opacity: 0.5;
      transition: opacity 300ms linear;
    }
    h1, h2, h3, h4, h5, h6,
    .h1, .h2, .h3, .h4, .h5, .h6,
    .display-1, .display-2, .display-3,
    .display-4, .display-5, .display-6 {
      font-family: "Anonymous Pro", sans-serif;
      font-weight: bold
    }
    html, body, .tooltip, .popover {
      font-family: 'Anonymous Pro', sans-serif;
      font-variant-emoji: emoji;
    }
    code, kbd, pre, samp, .text-monospace {
      font-family: 'Anonymous Pro', monospace;
      font-variant-emoji: emoji;
    }
  </style>
</head>
<body>
<div class="container-fluid py-3">
  <h1 class="text-center mb-4">WebDataset Index Browser</h1>
  <div class="row">
    <div class="col-md-4">
      <div id="file-tree" class="border rounded bg-white p-2">
        {{ body }}
      </div>
    </div>
    <div class="col-md-8">
      <div id="table-container" class="border rounded bg-white p-2">
      </div>
    </div>
  </div>
</div>
<script src="https://unpkg.com/htmx.org@2.0.4"></script>
</body>
</html>
"""


def render_body(html):
    return render_template_string(BASE_TEMPLATE, body=Markup(html))


# List directories and .utix files with LRU cache + inotify eviction


def list_directory(path):
    if path in dir_cache:
        return dir_cache[path]

    full_path = os.path.join(BASE_DIR, path)
    entries = []
    for name in os.listdir(full_path):
        rel_path = os.path.join(path, name) if path else name
        if os.path.isdir(os.path.join(full_path, name)):
            entries.append(("dir", name, rel_path))
        elif name.endswith(".utix"):
            entries.append(("file", name, rel_path))

    entries.sort()

    # Cache and watch this directory
    dir_cache[path] = entries
    if path not in watched_dirs:
        observer.schedule(
            CacheEvictHandler(dir_cache, path), full_path, recursive=False
        )
        watched_dirs.add(path)
    return entries


@app.route("/")
def index():
    entries = list_directory("")
    html = '<ul class="list-group list-group-flush">'
    for typ, name, rel in entries:
        icon = "📁" if typ == "dir" else "📄"
        endpoint = "browse" if typ == "dir" else "load"
        html += (
            f'<li class="list-group-item py-1">'
            f'<a href="#" hx-get="/{endpoint}?path={rel}" '
            f'hx-target="#file-tree" hx-indicator="#file-tree" hx-swap="innerHTML">{icon} {name}</a></li>'
        )
    html += "</ul>"
    return render_body(html)


class Map:
    def __init__(self, file):
        self.f = open(file, "r+b")
        self.mm = mmap.mmap(self.f.fileno(), 0)

    def __del__(self):
        self.mm.close()
        self.f.close()

    @staticmethod
    def open(file: str):
        if file in mapfile_cache:
            return mapfile_cache[file]
        m = Map(file)
        mapfile_cache[file] = m
        return m


def _map_file(file: str, base: int, end: int) -> bytes:
    m = Map.open(file)
    return m.mm[base:end]


import mimetypes

mimetypes.init()


def map_file_internal(file, base, end):
    full_path = os.path.join(BASE_DIR, file)
    base = int(base, 16)
    end = int(end, 16)

    b = _map_file(full_path, base, end)

    return b


@app.route("/boxed_file")
def boxed_file():
    file = request.args.get("file")
    base = request.args.get("base")
    end = request.args.get("end")
    k = request.args.get("k")
    if file is None or base is None or end is None or k is None:
        return "Missing arguments"

    em = f'<div><a href="/map_file?file={quote(file, safe=[])}&k={k}&base={base}&end={end}" target="_blank">Open {k}</a></div>'

    mime_type, _ = mimetypes.guess_type("test" + k, False)
    if mime_type:
        if mime_type.startswith("image"):
            em += f'<img src="/map_file?file={quote(file, safe=[])}&k={k}&base={base}&end={end}" class="img-fluid" style="max-height: 128px;"/>'
        if mime_type in ["text/plain", "text/html", "application/json"]:
            em += f'<div class="border p-1" style="width: auto; max-width: 20em; max-height: 10em; overflow: auto;"><pre><code>{map_file_internal(file, base, end).decode("utf-8")}</code></pre>'

    return em


@app.route("/map_file")
def map_file():
    file = request.args.get("file")
    base = request.args.get("base")
    end = request.args.get("end")
    k = request.args.get("k")
    if file is None or base is None or end is None or k is None:
        return "Missing arguments"

    response = make_response(map_file_internal(file, base, end))
    mime_type, _ = mimetypes.guess_type("test" + k, False)
    print("MIME type:", mime_type)
    if mime_type:
        response.mimetype = mime_type

    return response


@app.route("/browse")
def browse():
    path = request.args.get("path", "")
    entries = list_directory(path)
    html = '<ul class="list-group list-group-flush">'
    if path:
        parent = os.path.dirname(path)
        html += (
            f'<li class="list-group-item py-1">'
            f'<a href="#" hx-get="/browse?path={parent}" '
            f'hx-target="#file-tree" hx-indicator="#file-tree" hx-swap="innerHTML">⬆️ ..</a></li>'
        )
    for typ, name, rel in entries:
        icon = "📁" if typ == "dir" else "📄"
        endpoint = "browse" if typ == "dir" else "load"
        target = "#file-tree" if typ == "dir" else "#table-container"
        html += (
            f'<li class="list-group-item py-1">'
            f'<a href="#" hx-get="/{endpoint}?path={rel}" '
            f'hx-target="{target}" hx-indicator="{target}" hx-swap="innerHTML">{icon} {name}</a></li>'
        )
    html += "</ul>"
    return html


@app.route("/load")
def load_index():
    path = request.args.get("path")
    # No caching here — just load fresh unpacked data
    data = []
    links = []
    full_path = os.path.join(BASE_DIR, path)
    tar_path = full_path.removesuffix(".utix")
    with open(full_path, "rb") as f:
        unpacker = msgpack.Unpacker(f, raw=False)
        for item in unpacker:
            l = {}
            base_offset = item["offset"]
            d = {"id": item["str_idx"], "idx": item["iidx"]}
            for k, o, s in zip(item["keys"], item["offsets"], item["sizes"]):
                off = base_offset + o
                size = s
                d[k] = f"{off:08X}..{off+size:08X}"
                l[
                    k
                ] = f"/boxed_file?file={quote(tar_path, safe=[])}&k={k}&base={off:x}&end={off+size:x}"
            data.append(d)
            links.append(l)

    if not data:
        return "<p>No items found in the index.</p>"

    keys = sorted(data[0].keys())
    html = '<div class="table-responsive" style="max-height: calc(100vh - 200px); overflow-y: auto;">'
    html += (
        f"<p>{tar_path}</p>"
        '<table class="table table-hover table-sm mb-0"><thead class="table-light"><tr>'
    )
    for k in keys:
        html += f"<th>{k}</th>"
    html += "</tr></thead><tbody>"
    for idx, (row, l) in enumerate(zip(data, links)):
        html += f'<tr class="selectable-row" data-index="{idx}">'
        for k in keys:
            if k in l:
                href = l.get(k)
                html += (
                    f'<td><a hx-get="{href}" hx-swap="outerHTML">{row.get(k)}</a></td>'
                )
            else:
                html += f"<td>{row.get(k, '')}</td>"
        html += "</tr>"
    html += "</tbody></table></div>"
    return html


if __name__ == "__main__":
    app.run(debug=True)
