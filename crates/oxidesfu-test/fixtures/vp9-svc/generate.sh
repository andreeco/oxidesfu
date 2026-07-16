#!/usr/bin/env sh
# Regenerate the independently decodable VP9 keyframes used by the native
# dependency-descriptor forwarding contract. Each output contains one IVF
# frame; the 32-byte IVF file header and 12-byte frame header are removed.
#
# Requires ffmpeg built with libvpx-vp9. The test supplies descriptor metadata
# separately through NativeVideoSource::capture_svc_encoded_frame.
set -eu

ffmpeg -hide_banner -loglevel error \
  -f lavfi -i color=c=red:s=320x180:d=0.04 \
  -frames:v 1 -c:v libvpx-vp9 -deadline best -cpu-used 8 \
  -g 1 -keyint_min 1 -row-mt 0 -tile-columns 0 -crf 50 -b:v 0 \
  -f ivf crates/oxidesfu-test/fixtures/vp9-svc/low.ivf

dd if=crates/oxidesfu-test/fixtures/vp9-svc/low.ivf of=crates/oxidesfu-test/fixtures/vp9-svc/low-keyframe.vp9 bs=1 skip=44 status=none
rm crates/oxidesfu-test/fixtures/vp9-svc/low.ivf

ffmpeg -hide_banner -loglevel error \
  -f lavfi -i color=c=blue:s=1280x720:d=0.04 \
  -frames:v 1 -c:v libvpx-vp9 -deadline best -cpu-used 8 \
  -g 1 -keyint_min 1 -row-mt 0 -tile-columns 0 -crf 50 -b:v 0 \
  -f ivf crates/oxidesfu-test/fixtures/vp9-svc/high.ivf

dd if=crates/oxidesfu-test/fixtures/vp9-svc/high.ivf of=crates/oxidesfu-test/fixtures/vp9-svc/high-keyframe.vp9 bs=1 skip=44 status=none
rm crates/oxidesfu-test/fixtures/vp9-svc/high.ivf
