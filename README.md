# Ultar

> Ultra-scale tar / webdataset

> [!NOTE]
> No gurantees whatsoever.
> We are just trying to make webdataset random-accessible,
> And make the indexing fast as frick

## Contribute / Develop

The project is built with `zig`. You should be able to build it with a `zig>=0.14` install.

Don't know how to install `zig`? This project is also setup with `pixi` to manage a `conda-forge` based environment.

Given that webdataset & ML is very python based, having `pixi` is a good idea anyways.

```sh
pixi shell
zig build -Doptimize=ReleaseFast
```

## Install

CI should provide a build for you. It should be fully static so no glibc requirements.

## Performance

Remote NFS storage (hosted by lambdalabs), scanning 32 tar files (~1.4GB each) with a single instance / process of `indexer`

```sh
        User time (seconds): 3.20
        System time (seconds): 72.28
        Percent of CPU this job got: 183%
        Elapsed (wall clock) time (h:mm:ss or m:ss): 0:41.09
        Maximum resident set size (kbytes): 10624
```

On this particular system a single instance of `indexer` saturates at around `10Gibps` with most of the time spent on Linux's NFS server.

It runs sligthly too fast for local NVMe storage so I didn't bother a instrumented test.

## Methodology

Simple single-process event loop based IO provided by `libxev` & thus wielding the full power of `IO_URING`.

Have I mentioned it's written with [zig](https://ziglang.org)
