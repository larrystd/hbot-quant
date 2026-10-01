"""Common settings for trading libraries under hquant/src."""

load("@rules_cc//cc:defs.bzl", "cc_library")

_WARNING_COPTS = ["-Werror=return-type"]

def hquant_cc_library(name, visibility = None, copts = None, **kwargs):
    cc_library(
        name = name,
        strip_include_prefix = "/hquant/src",
        copts = _WARNING_COPTS + (copts or []),
        visibility = visibility or ["//visibility:private"],
        **kwargs
    )
