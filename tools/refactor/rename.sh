#!/usr/bin/env bash
# Usage: ren.sh <map-file> [ext-regex]   (run from repo root)
# map-file lines: <perl-regex><TAB><replacement>; '#' lines ignored.
# Replacement may use $1..$9 for capture groups.
set -euo pipefail
export MAP="$1"
ext="${2:-cc|h|bazel|bzl|yaml|json|md}"
find hquant dev docs -type f | grep -E "\.(${ext})$" | tr '\n' '\0' |
xargs -0 perl -i -pe '
  BEGIN {
    open(my $fh, "<", $ENV{MAP}) or die "map: $!";
    while (<$fh>) {
      chomp; next if /^#/ || !/\t/;
      my ($k, $v) = split /\t/, $_, 2;
      push @R, [qr/$k/, $v];
    }
  }
  for my $r (@R) {
    my ($k, $v) = @$r;
    s{$k}{
      my @c = ($1, $2, $3, $4, $5, $6, $7, $8, $9);
      (my $out = $v) =~ s/\$(\d)/defined $c[$1 - 1] ? $c[$1 - 1] : ""/ge;
      $out
    }ge;
  }
'
