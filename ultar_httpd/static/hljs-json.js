let hljsPromise = null;
function loadHljs() {
  if (hljsPromise) return hljsPromise;
  hljsPromise = (async () => {
    try {
      const [{ default: hljs }, { default: json }] = await Promise.all([
        import('https://cdn.jsdelivr.net/npm/@highlightjs/cdn-assets@11.11.1/es/core.min.js'),
        import('https://cdn.jsdelivr.net/npm/@highlightjs/cdn-assets@11.11.1/es/languages/json.min.js')
      ]);
      hljs.registerLanguage('json', json);
      const css = document.createElement('style');
      css.textContent =
        '.hljs{color:var(--ctp-text);background:var(--ctp-crust);font-family:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,"Liberation Mono",monospace}' +
        '.hljs-keyword,.hljs-literal{color:var(--ctp-mauve);font-weight:600}' +
        '.hljs-string{color:var(--ctp-green)}' +
        '.hljs-number{color:var(--ctp-peach)}' +
        '.hljs-attr,.hljs-attribute,.hljs-property,.hljs-attr-name{color:var(--ctp-blue)}' +
        '.hljs-punctuation{color:var(--ctp-overlay2)}' +
        '.hljs-comment,.hljs-quote{color:var(--ctp-overlay1);font-style:italic}' +
        '.hljs-title,.hljs-section,.hljs-name{color:var(--ctp-sapphire)}' +
        '.hljs-built_in,.hljs-type,.hljs-class{color:var(--ctp-yellow)}' +
        '.hljs-variable,.hljs-template-variable{color:var(--ctp-red)}' +
        '.hljs-symbol,.hljs-bullet{color:var(--ctp-teal)}' +
        '.hljs-addition{color:var(--ctp-green);background:rgba(166,227,161,.10)}' +
        '.hljs-deletion{color:var(--ctp-red);background:rgba(243,139,168,.10)}' +
        '.hljs-strong{font-weight:700}' +
        '.hljs-emphasis{font-style:italic}';
      document.head.appendChild(css);
      return hljs;
    } catch (e) {
      console.warn('hljs load failed', e);
      return null;
    }
  })();
  return hljsPromise;
}

async function init(el) {
  const url = el.getAttribute('data-src');
  if (!url) return;
  const res = await fetch(url);
  const txt = await res.text();
  let out = txt;
  try { out = JSON.stringify(JSON.parse(txt), null, 2); } catch (e) {}
  const hljs = await loadHljs();
  if (hljs) {
    el.innerHTML = hljs.highlight(out, { language: 'json' }).value;
  } else {
    el.textContent = out;
  }
}

function scanRoot(root) {
  const els = root.matches?.('.json-code') ? [root] : root.querySelectorAll('.json-code');
  for (const el of els) {
    if (!el.hasAttribute('data-initialized')) {
      el.setAttribute('data-initialized', '1');
      init(el);
    }
  }
}

// Expose globally so app.js click handler can trigger JSON highlighting
window.scanRoot = scanRoot;
// htmx.onLoad fires on newly loaded content (e.g. table page changes)
htmx.onLoad(scanRoot);
// Initial scan for server-side pre-rendered content
scanRoot(document.body);
