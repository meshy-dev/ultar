const std = @import("std");

pub const block_size = 512;

pub const PosixMagic = extern struct {
    magic: [6]u8, // POSIX tar magic number
    version: [2]u8, // POSIX tar version

    const ref_magic = "ustar" ++ .{0};

    pub fn isValid(self: PosixMagic) bool {
        return std.mem.eql(u8, &self.magic, PosixMagic.ref_magic);
    }
};

pub const GnuMagic = extern struct {
    magic: [8]u8, // GNU tar magic number

    const ref_magic = "ustar  " ++ .{0};

    pub fn isValid(self: GnuMagic) bool {
        return std.mem.eql(u8, &self.magic, GnuMagic.ref_magic);
    }
};

pub const Magic = extern union { posix: PosixMagic, gnu: GnuMagic };

pub const TarHeader = struct {
    name: [100]u8, // File name
    mode: [8]u8, // File mode (octal ASCII)
    uid: [8]u8, // Owner user ID (octal ASCII)
    gid: [8]u8, // Owner group ID (octal ASCII)
    size: [12]u8, // File size in bytes (octal ASCII)
    mtime: [12]u8, // Modification time (octal ASCII)
    chksum: [8]u8, // Checksum (ASCII space‑filled for sum calculation)
    typeflag: u8, // File type indicator
    linkname: [100]u8, // Name of linked file (for hard/sym links)
    magic: Magic, // Magic number (POSIX or GNU)
    uname: [32]u8, // Owner user name
    gname: [32]u8, // Owner group name
    devmajor: [8]u8, // Device major number (for character/block devices)
    devminor: [8]u8, // Device minor number
    prefix: [155]u8, // Path prefix to allow 255‑byte names
    pad: [12]u8, // Padding to fill 512 bytes
};

comptime {
    std.debug.assert(@sizeOf(TarHeader) == block_size);
}

pub fn calcChecksum(header: *const TarHeader, comptime char_type: type) u32 {
    var sum: u32 = ' ' * 8;
    const header_bytes: *const [512]char_type = @ptrCast(header);
    for (0..@offsetOf(TarHeader, "chksum")) |i| {
        sum += @intCast(header_bytes[i]);
    }
    for (header_bytes[@offsetOf(TarHeader, "chksum") + 8 ..]) |c| {
        sum += @intCast(c);
    }

    return sum;
}

pub fn isZeroBlock(header: *const TarHeader) bool {
    const header_bytes: *const [512]u8 = @ptrCast(header);
    return std.mem.allEqual(u8, header_bytes, 0);
}
