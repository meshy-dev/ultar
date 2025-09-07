const std = @import("std");

// Renders a JSON preview. No <script>/<style> here; we rely on a one-time
// setup snippet added to <body> via HTMX from the server. The runtime script
// fetches the data-src and highlights it using highlight.js.
// Ref: highlight.js usage [https://highlightjs.org/]
pub fn render(alloc: std.mem.Allocator, src: []const u8) ![]const u8 {
    const html =
        \\<div class="json-preview" style="margin-top:6px">
        \\  <div hx-get="/assets/preview/json/setup" hx-trigger="load, once" hx-target="body" hx-swap="beforeend"></div>
        \\  <pre><code class="language-json json-code" data-src="%SRC%" style="display:block; width:100%; max-width:720px; max-height:360px; overflow:auto; border:1px solid var(--border); padding:8px; background:var(--ctp-crust);"></code></pre>
        \\</div>
    ;
    return std.mem.replaceOwned(u8, alloc, html, "%SRC%", src);
}
