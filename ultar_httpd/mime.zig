const std = @import("std");

fn stripDot(ext: []const u8) []const u8 {
    return if (ext.len > 0 and ext[0] == '.') ext[1..] else ext;
}

/// MIME for `/static/` assets; text formats carry `; charset=utf-8`.
pub fn forStaticExt(ext: []const u8) []const u8 {
    const e = stripDot(ext);
    if (std.ascii.eqlIgnoreCase(e, "js")) return "application/javascript; charset=utf-8";
    if (std.ascii.eqlIgnoreCase(e, "css")) return "text/css; charset=utf-8";
    return "application/octet-stream";
}

/// MIME for `/map_file` payloads; no charset suffix.
pub fn forFileExt(ext: []const u8) []const u8 {
    const e = stripDot(ext);

    if (std.ascii.eqlIgnoreCase(e, "json")) return "application/json";

    if (std.ascii.eqlIgnoreCase(e, "jpg")) return "image/jpeg";
    if (std.ascii.eqlIgnoreCase(e, "jpeg")) return "image/jpeg";
    if (std.ascii.eqlIgnoreCase(e, "png")) return "image/png";
    if (std.ascii.eqlIgnoreCase(e, "gif")) return "image/gif";
    if (std.ascii.eqlIgnoreCase(e, "webp")) return "image/webp";
    if (std.ascii.eqlIgnoreCase(e, "jxl")) return "image/jxl";
    if (std.ascii.eqlIgnoreCase(e, "bmp")) return "image/bmp";
    if (std.ascii.eqlIgnoreCase(e, "svg")) return "image/svg+xml";

    if (std.ascii.eqlIgnoreCase(e, "mp4")) return "video/mp4";
    if (std.ascii.eqlIgnoreCase(e, "webm")) return "video/webm";
    if (std.ascii.eqlIgnoreCase(e, "mov")) return "video/quicktime";
    if (std.ascii.eqlIgnoreCase(e, "avi")) return "video/x-msvideo";

    if (std.ascii.eqlIgnoreCase(e, "mp3")) return "audio/mpeg";
    if (std.ascii.eqlIgnoreCase(e, "wav")) return "audio/wav";
    if (std.ascii.eqlIgnoreCase(e, "ogg")) return "audio/ogg";
    if (std.ascii.eqlIgnoreCase(e, "flac")) return "audio/flac";

    if (std.ascii.eqlIgnoreCase(e, "txt")) return "text/plain";
    if (std.ascii.eqlIgnoreCase(e, "csv")) return "text/csv";
    if (std.ascii.eqlIgnoreCase(e, "log")) return "text/plain";
    if (std.ascii.eqlIgnoreCase(e, "xml")) return "application/xml";
    if (std.ascii.eqlIgnoreCase(e, "yaml")) return "application/yaml";
    if (std.ascii.eqlIgnoreCase(e, "yml")) return "application/yaml";
    if (std.ascii.eqlIgnoreCase(e, "md")) return "text/markdown";

    return "application/octet-stream";
}
