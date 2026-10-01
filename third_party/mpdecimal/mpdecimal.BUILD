# libmpdec 4.0.1（CPython decimal 的底层实现），BSD-2 许可证，见 COPYRIGHT.txt。
# 对应 configure 在 64 位 + 128 位整数平台上的选择：-DCONFIG_64 -DANSI -DHAVE_UINT128_T，
# 覆盖 macOS arm64 与 Linux x86_64，不使用 x86 专用汇编。

load("@rules_cc//cc:defs.bzl", "cc_library")

genrule(
    name = "mpdecimal_h",
    srcs = ["libmpdec/mpdecimal.h.in"],
    outs = ["libmpdec/mpdecimal.h"],
    cmd = """cat > $(@D)/config.txt <<'CFG'
/* ABI: 64-bit */
#define MPD_CONFIG_64 1

#ifdef MPD_CONFIG_32
  #error "cannot use MPD_CONFIG_32 with 64-bit header."
#endif

#ifdef CONFIG_32
  #error "cannot use CONFIG_32 with 64-bit header."
#endif
CFG
awk -v cfg="$(@D)/config.txt" '/@MPD_HEADER_CONFIG@/ { while ((getline line < cfg) > 0) print line; next } { print }' $< > $@""",
)

cc_library(
    name = "mpdecimal",
    srcs = [
        "libmpdec/basearith.c",
        "libmpdec/constants.c",
        "libmpdec/context.c",
        "libmpdec/convolute.c",
        "libmpdec/crt.c",
        "libmpdec/difradix2.c",
        "libmpdec/fnt.c",
        "libmpdec/fourstep.c",
        "libmpdec/io.c",
        "libmpdec/mpalloc.c",
        "libmpdec/mpdecimal.c",
        "libmpdec/mpsignal.c",
        "libmpdec/numbertheory.c",
        "libmpdec/sixstep.c",
        "libmpdec/transpose.c",
    ] + glob(
        ["libmpdec/*.h"],
        exclude = ["libmpdec/mpdecimal32vc.h", "libmpdec/mpdecimal64vc.h"],
    ),
    hdrs = [":mpdecimal_h"],
    copts = [
        "-DCONFIG_64",
        "-DANSI",
        "-DHAVE_UINT128_T",
        "-Wno-unused-parameter",
    ],
    includes = ["libmpdec"],
    visibility = ["//visibility:public"],
)
