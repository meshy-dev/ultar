/* ── Content type detection ─────────────────────────────────── */
var _imgExts = {jpg:1,jpeg:1,png:1,gif:1,webp:1,bmp:1,svg:1};
var _vidExts = {mp4:1,webm:1,mov:1,avi:1};
var _audExts = {mp3:1,wav:1,ogg:1,flac:1};
var _txtExts = {txt:1,csv:1,log:1,xml:1,yaml:1,yml:1,md:1};
function contentType(key) {
  var k = decodeURIComponent(key);
  var dot = k.lastIndexOf('.');
  var ext = dot >= 0 ? k.slice(dot + 1).toLowerCase() : '';
  if (_imgExts[ext]) return 'image';
  if (ext === 'json') return 'json';
  if (_vidExts[ext]) return 'video';
  if (_audExts[ext]) return 'audio';
  if (_txtExts[ext]) return 'text';
  return 'other';
}

function buildUrl(file, k, rangeStr, id) {
  return '/map_file?file=' + file + '&k=' + k
    + '&range_str=' + encodeURIComponent(rangeStr)
    + '&id=' + encodeURIComponent(id);
}

/* ── Floating window stacking ──────────────────────────────── */
var _zTop = 100;
function bringToFront(win) { win.style.zIndex = ++_zTop; }

function positionWindow(win) {
  var off = (document.querySelectorAll('#windows > .float-window').length - 1) * 20;
  win.style.left = 'calc(50% - 12rem + ' + off + 'px)';
  win.style.top = (80 + off) + 'px';
  bringToFront(win);
}

function openFloat(tplId, url, title) {
  var tpl = document.getElementById(tplId);
  var clone = tpl.content.cloneNode(true);
  var win = clone.querySelector('.float-window');
  win.querySelector('.float-title').textContent = title;
  win.querySelector('.float-dl').href = url;
  return { clone: clone, win: win };
}

/* ── Cell click handler ────────────────────────────────────── */
document.body.addEventListener('click', function(e) {
  /* Image thumbnail -> open floating window */
  var thumb = e.target.closest('.image-preview');
  if (thumb && thumb.closest('table.table')) {
    e.preventDefault();
    var url = thumb.dataset.url;
    var title = thumb.dataset.title;
    var f = openFloat('tpl-float-image', url, title);
    var body = f.win.querySelector('.float-body');
    body.querySelector('a').href = url;
    body.querySelector('img').src = url;
    positionWindow(f.win);
    document.getElementById('windows').appendChild(f.clone);
    return;
  }

  /* Regular cell <a> click */
  var a = e.target.closest('table.table td a');
  if (!a || a.classList.contains('image-preview')) return;
  e.preventDefault();

  var td = a.closest('td');
  var tr = td.parentElement;
  var tbl = td.closest('table');
  var th = tbl.querySelector('thead tr').children[td.cellIndex];
  if (!th || !th.dataset.k) return;

  var file = tbl.dataset.file;
  var k = th.dataset.k;
  var rangeStr = a.textContent.trim();
  var id = tr.dataset.id;
  var url = buildUrl(file, k, rangeStr, id);
  var type = contentType(k);
  var title = id + ' \u00b7 ' + decodeURIComponent(k);

  if (type === 'image') {
    /* Replace cell with inline thumbnail */
    var tpl = document.getElementById('tpl-thumb');
    var clone = tpl.content.cloneNode(true);
    var preview = clone.querySelector('.image-preview');
    preview.querySelector('img').src = url;
    preview.dataset.url = url;
    preview.dataset.title = title;
    a.replaceWith(clone);
    return;
  }

  /* Open floating window for non-image types */
  var f = openFloat('tpl-float-' + type, url, title);
  var win = f.win;
  var body = win.querySelector('.float-body');

  if (type === 'json') {
    body.querySelector('.json-code').setAttribute('data-src', url);
  } else if (type === 'video') {
    body.querySelector('video').src = url;
  } else if (type === 'audio') {
    body.querySelector('audio').src = url;
  } else if (type === 'text') {
    body.querySelector('iframe').src = url;
  }

  positionWindow(win);
  document.getElementById('windows').appendChild(f.clone);

  if (type === 'json' && window.scanRoot) window.scanRoot(body);
});

/* ── Drag handler ──────────────────────────────────────────── */
(function() {
  var dragging = null, startX, startY, origX, origY;
  document.addEventListener('mousedown', function(e) {
    var bar = e.target.closest('.float-titlebar');
    if (!bar || e.target.closest('.float-close') || e.target.closest('.float-dl')) return;
    var win = bar.closest('.float-window');
    dragging = win;
    bringToFront(win);
    startX = e.clientX; startY = e.clientY;
    var rect = win.getBoundingClientRect();
    origX = rect.left; origY = rect.top;
    e.preventDefault();
  });
  document.addEventListener('mousemove', function(e) {
    if (!dragging) return;
    dragging.style.left = (origX + e.clientX - startX) + 'px';
    dragging.style.top = (origY + e.clientY - startY) + 'px';
  });
  document.addEventListener('mouseup', function() { dragging = null; });
  /* Bring to front on click anywhere on the window */
  document.addEventListener('mousedown', function(e) {
    var win = e.target.closest('.float-window');
    if (win) bringToFront(win);
  });
})();
