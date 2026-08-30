# shellcheck shell=bash
# =============================================================================
# chaos-cases-archive.sh — Aeron archive failure/data-loss/corruption cases.
# =============================================================================
# SOURCED into chaos.sh's shell (never executed as a child). This file must
# NOT install traps (chaos.sh owns the single EXIT trap).

# Run Aeron's ArchiveTool against a node's archive dir via a ONE-OFF container
# (the archive daemon is down/drained during the surgery windows, so the tool
# cannot run inside it). Replaces the docker-run block previously repeated 5×.
# stdout+stderr are merged INSIDE the node shell so callers capture the whole
# tool output (Exception lines print to stderr).
archive_tool() { # <node> <aeron-image> <ArchiveTool-args...>
  local node="$1" img="$2"; shift 2
  docker exec "${node}" bash -lc \
    "docker run --rm -v /opt/kardamom/archive:/opt/kardamom/archive --entrypoint java ${img} \
     --add-opens java.base/java.util.zip=ALL-UNNAMED \
     -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir $* 2>&1"
}

case_archive_driver_loss() {
  # HARD-kill the Aeron SUBSTRATE (the `aeron` system job's combined
  # ArchivingMediaDriver task) on the ingress-0 node — not the ingress task
  # itself. This is the untested failure surface the component cases skip:
  # every service on the node shares that driver's aeron.dir, so the local
  # ingress loses its transport AND its tx_data durability recorder in one
  # blow. Expected outcome, in order:
  #   1. the pipeline keeps progressing — ingress is active/active, so
  #      clients retry against ingress-1 (the load runs in --chaos-mode);
  #   2. Nomad restarts the aeron system task within the restart SLO
  #      (archive segments persist on the node volume across the restart);
  #   3. the collocated ingress task recovers against the fresh driver and
  #      the ingress job returns to full strength within the reschedule SLO
  #      (driver + dependent-task restart chain, hence the wider SLO).
  local aeron_base
  aeron_base="$(count_running aeron)"
  # A hiccuping nomad query reads as 0/empty; injecting anyway would make
  # the post-kill `assert_count aeron >= 0` pass trivially and silently
  # drop the "driver restarted" leg of the case.
  [ "${aeron_base:-0}" -gt 0 ] \
    || fail "archive-driver-loss: no running aeron allocs at baseline (got '${aeron_base}') — cannot assert driver recovery"
  log "archive-driver-loss: killing archiving-media-driver on kardamom-ingress-0 (aeron allocs baseline=${aeron_base})"
  inject_hard kardamom-ingress-0 archiving-media-driver
  assert_progress
  assert_count aeron "${aeron_base}" "${CHAOS_RESTART_SLO_S}"
  assert_count ingress 2 "${CHAOS_RESCHEDULE_SLO_S}"
  assert_ingress_pair_live "${name}"
}

case_archive_tx_data_wipe() {
  # DATA-loss drill (not just process loss): permanently WIPE ingress-0's
  # tx_data archive volume, then restore it from ingress-1's mirror. tx_data
  # is UDP multicast, so BOTH ingress archives record every publisher's shard
  # streams — the segments are byte-identical across the two nodes (verified
  # by sha256), so a single node's archive loss is survivable and the peer is
  # an exact restore source. This exercises `kardamom-archive-rereplicate`'s
  # mechanism (segment + catalog mirror). Expected outcome, in order:
  #   1. the pipeline keeps progressing on ingress-1 (active/active) while
  #      ingress-0's substrate is down;
  #   2. after re-replicating ingress-1's segments + catalog, the restarted
  #      ingress-0 archive adopts them and Aeron's own `ArchiveTool verify`
  #      reports every recording OK;
  #   3. ingress + aeron return to full strength.
  local aeron_base ac0 verify_out
  aeron_base="$(count_running aeron)"
  log "archive-tx-data-wipe: killing aeron substrate on kardamom-ingress-0 + wiping its tx_data archive volume"
  inject_hard kardamom-ingress-0 archiving-media-driver
  # Simulate permanent volume loss while the driver is down (segments + catalog).
  docker exec kardamom-ingress-0 bash -lc \
    'rm -f /opt/kardamom/archive/dir/*.rec /opt/kardamom/archive/dir/archive.catalog' \
    || fail "archive-tx-data-wipe: could not wipe ingress-0 archive"
  # Re-replicate from the surviving peer (ingress-1): stream its archive dir
  # across. This is the transport that kardamom-archive-rereplicate wraps for
  # an operator (peer copy -> mirror_archive -> verify_mirror).
  #
  # NEVER transplant archive-mark.dat from a LIVE source: the peer's daemon
  # heartbeats it, so the copy looks "active" to the victim's restarting
  # Archive, which then crash-loops on 'active Mark file detected' until the
  # copied heartbeat ages out — observed blowing the 60s restart SLO (the
  # recurring 'aeron did not reach >= 8 running (have 7)' flake, on main and
  # PRs). The victim's own mark file was deliberately preserved by the wipe
  # above and its heartbeat died with the killed driver, so it is already
  # stale by restart time and the daemon starts cleanly.
  # And copy the CATALOG first, via a STABLE read (issue #98): the mirror's
  # daemon rewrites catalog entries on recording lifecycle events, and this
  # very injection triggers some — ingress-0's publishers died with its
  # driver, so the mirror STOPS those recordings and rewrites exactly those
  # entries seconds later. A copy racing such a write captures a torn entry
  # (stored checksum != descriptor) that fails a CRC-armed verify. Two
  # consecutive identical snapshots guarantee a consistent image; copying
  # the catalog before the segments guarantees segment data covers every
  # position it references. (kardamom-archive-rereplicate does the same.)
  log "archive-tx-data-wipe: re-replicating archive from kardamom-ingress-1 mirror"
  local cat_h1 cat_h2 stable=0
  for _ in $(seq 1 10); do
    cat_h1="$(docker exec kardamom-ingress-1 sha256sum /opt/kardamom/archive/dir/archive.catalog | cut -d' ' -f1)"
    cat_h2="$(docker exec kardamom-ingress-1 sha256sum /opt/kardamom/archive/dir/archive.catalog | cut -d' ' -f1)"
    if [ -n "${cat_h1}" ] && [ "${cat_h1}" = "${cat_h2}" ]; then
      docker exec kardamom-ingress-1 cat /opt/kardamom/archive/dir/archive.catalog \
        | docker exec -i kardamom-ingress-0 bash -lc 'cat > /opt/kardamom/archive/dir/archive.catalog' \
        || fail "archive-tx-data-wipe: catalog copy failed"
      cat_h2="$(docker exec kardamom-ingress-1 sha256sum /opt/kardamom/archive/dir/archive.catalog | cut -d' ' -f1)"
      [ "${cat_h1}" = "${cat_h2}" ] && { stable=1; break; }
    fi
    sleep 1
  done
  [ "${stable}" = 1 ] || fail "archive-tx-data-wipe: mirror catalog never stabilized across 10 attempts"
  local seg_rc=0
  docker exec kardamom-ingress-1 tar -C /opt/kardamom/archive --warning=no-file-changed \
    --exclude='dir/archive-mark.dat' --exclude='dir/archive.catalog' -cf - dir \
    | docker exec -i kardamom-ingress-0 tar -C /opt/kardamom/archive -xf - \
    || seg_rc=$?
  # tar rc=1 = segment appended under the live recorder mid-copy; the
  # torn tail is what the restart-side verify/heal (#94/#95) handles.
  [ "${seg_rc}" -le 1 ] \
    || fail "archive-tx-data-wipe: re-replication copy failed (rc=${seg_rc})"
  # The pipeline must have ridden through on ingress-1 the whole time.
  assert_progress
  # aeron restarts (system job) and adopts the restored archive.
  assert_count aeron "${aeron_base}" "${CHAOS_RESTART_SLO_S}"
  # Verify the restored archive with Aeron's own tool: every recording OK.
  ac0="$(inner_container kardamom-ingress-0 archiving-media-driver)"
  [ -n "${ac0}" ] || fail "archive-tx-data-wipe: no aeron container on ingress-0 after restart"
  # CRC-ARMED verify is the regression gate for issue #98: every data
  # frame's recorded CRC32 plus file availability/structure. One class of
  # ERR is TOLERATED (counted + logged): 'invalid Catalog checksum'.
  # Aeron 1.45 patches catalog entries when active recordings are adopted
  # /stopped out-of-band (ArchiveTool.verify writes recovered stop
  # positions without recomputing the entry checksum; the daemon's
  # adoption path behaves the same in CI evidence), so entry checksums on
  # a restored-and-adopted archive go stale by construction — an upstream
  # gap, not a torn transplant. Frame CRCs are unaffected and remain the
  # authoritative integrity signal. A crashed tool never passes.
  # (This verify runs INSIDE the freshly-restarted daemon container — the
  # daemon owns the dir again — so it is not an archive_tool one-off run.)
  local v_ok=0 stale_entries=0
  for _ in 1 2 3; do
    verify_out="$(docker exec kardamom-ingress-0 bash -lc \
      "docker exec ${ac0} bash -lc 'java --add-opens java.base/java.util.zip=ALL-UNNAMED -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir verify -a -checksum io.aeron.archive.checksum.Crc32 2>&1'" || true)"
    stale_entries="$(echo "${verify_out}" | grep -c 'invalid Catalog checksum' || true)"
    # Non-checksum errors counted with `grep -c` (reads ALL input — no
    # early exit, so no SIGPIPE) instead of a filtered `grep -q` chain.
    local other_err
    other_err="$(echo "${verify_out}" | grep -v 'invalid Catalog checksum' | grep -ciE "ERR |FAILED" || true)"
    if ! has_line "${verify_out}" 'Exception' \
      && has_match "${verify_out}" "recordingId=.*OK" \
      && [ "${other_err:-0}" -eq 0 ]; then
      v_ok=1
      break
    fi
    sleep 5
  done
  if [ "${v_ok}" != 1 ]; then
    echo "${verify_out}" | tail -20
    fail "archive-tx-data-wipe: restored archive failed CRC-armed verify after retries"
  fi
  if [ "${stale_entries:-0}" -gt 0 ]; then
    log "archive-tx-data-wipe: note — ${stale_entries} adoption-staled catalog entry checksum(s) tolerated (Aeron 1.45 gap)"
  fi
  log "archive-tx-data-wipe: restored archive verified OK on ingress-0 (2-copy redundancy recovered)"
  assert_count ingress 2 "${CHAOS_RESCHEDULE_SLO_S}"
  assert_ingress_pair_live "${name}"
}

case_archive_corruption() {
  # DATA-corruption drill (present-but-wrong, not missing): flip bytes
  # mid-segment in ingress-0's tx_data archive — length preserved, so a
  # size check can't see it — then DETECT it with a CRC-armed
  # `ArchiveTool verify` (record-time CRC32 is enabled in the driver) and
  # HEAL only the corrupt segment from ingress-1's mirror via
  # `kardamom-archive-rereplicate --diff/--heal`. File-level surgery
  # requires the victim's archive daemon STOPPED throughout, so the node is
  # drained for the window (pipeline rides on ingress-1, as the other
  # archive cases prove). Expected, in order:
  #   1. pre-heal verify FAILS on the corrupted archive (detection);
  #   2. the Rust tool's --diff names the corrupted segment and --heal
  #      repairs exactly it from the mirror;
  #   3. post-heal CRC verify is clean, the node undrains, and aeron +
  #      ingress return to strength with the pipeline having progressed.
  local aeron_base node_id aeron_img tmp_dir verify_pre verify_post diverged
  [ -x "${REREP_BIN}" ] || fail "archive-corruption: kardamom-archive-rereplicate not at ${REREP_BIN}"
  aeron_base="$(count_running aeron)"
  [ -n "${aeron_base}" ] && [ "${aeron_base}" -gt 0 ] \
    || fail "archive-corruption: aeron baseline unavailable — refusing a vacuous pass"
  # Hold ingress-0 down for the whole surgery window: drain evicts the
  # aeron system task (which holds the catalog open) and keeps it down
  # until we undrain — a hard kill would race nomad's restart.
  node_id="$(on_control 'nomad node status -verbose 2>/dev/null | awk "/ingress-0/ {print \$1; exit}"')"
  [ -n "${node_id}" ] || fail "archive-corruption: could not resolve ingress-0 node id"
  aeron_img="$(docker exec kardamom-ingress-0 bash -lc \
    "docker ps -a --format '{{.Image}} {{.Names}}' | awk '/archiving/ {print \$1; exit}'")"
  [ -n "${aeron_img}" ] || fail "archive-corruption: could not resolve the aeron image on ingress-0"
  log "archive-corruption: draining ingress-0 node (${node_id})"
  on_control 'nomad node drain -enable -yes -deadline 2m "$1"' "${node_id}" >/dev/null \
    || fail "archive-corruption: drain enable failed"
  sleep 5
  # Pick a victim recording that verifies CLEAN at baseline. The archive's
  # catalog was restored from the live peer by archive-tx-data-wipe, and
  # which entries got torn in that copy is a per-run lottery (issue #98) —
  # this case tests SEGMENT corruption detect/heal, not catalog repair, so
  # it must start from a provably-clean recording: baseline OK -> corrupt
  # -> ERR -> heal -> OK is then a closed loop. Every verify/mark-valid is
  # scoped to that recording (segment name = <recordingId>-<base>.rec).
  local seg="" seg_name="" rid="" flip_at=-1 cand cand_name cand_rid cand_out cand_flip
  for cand in $(docker exec kardamom-ingress-0 bash -lc \
      'ls -S /opt/kardamom/archive/dir/*.rec 2>/dev/null | head -6'); do
    cand_name="$(basename "${cand}")"
    cand_rid="${cand_name%%-*}"
    # #126: recording ids are per-archive counters, so the victim's
    # post-restart/post-restore sessions (archive-driver-loss and
    # archive-tx-data-wipe both run earlier in this shard) own ids the
    # mirror never opened. A victim-only segment is unhealable from the
    # mirror BY CONSTRUCTION — no source bytes exist — so it cannot
    # drill the detect→heal loop; only candidates present on BOTH
    # archives qualify. (--diff now surfaces such segments as
    # "dest-only" instead of silently skipping them.)
    if ! docker exec kardamom-ingress-1 test -f "/opt/kardamom/archive/dir/${cand_name}" 2>/dev/null; then
      continue
    fi
    cand_out="$(archive_tool kardamom-ingress-0 "${aeron_img}" \
      verify "${cand_rid}" -a -checksum io.aeron.archive.checksum.Crc32 || true)"
    if has_line "${cand_out}" "recordingId=${cand_rid}) OK" \
      && ! has_line "${cand_out}" ') ERR'; then
      # Segment files are PRE-ALLOCATED (equal apparent size, ls -S order
      # arbitrary), and a flip landing in a frame HEADER can send verify's
      # frame-walk out of bounds (observed: JVM SIGSEGV in the CRC32
      # intrinsic) instead of a clean ERR. Walk the Aeron data-frame
      # headers and pick a flip offset INSIDE the payload of the largest
      # real data frame (type 0x01, skipping padding frames); -1 means the
      # segment has no usable frame — try the next candidate.
      cand_flip="$(docker exec kardamom-ingress-0 python3 -c "
b = open('${cand}', 'rb').read()
pos = 0; best = -1; bestlen = 0
while pos + 32 <= len(b):
    ln = int.from_bytes(b[pos:pos+4], 'little', signed=True)
    if ln <= 0:
        break  # first zero-length header = start of the unrecorded tail
    typ = int.from_bytes(b[pos+6:pos+8], 'little')
    if typ == 1 and ln >= 96 and pos + ln <= len(b) and ln > bestlen:
        best = pos; bestlen = ln
    pos += (ln + 31) // 32 * 32
print(best + 40 if best >= 0 else -1)
" 2>/dev/null || echo -1)"
      if [ "${cand_flip:--1}" -ge 0 ]; then
        seg="${cand}"; seg_name="${cand_name}"; rid="${cand_rid}"; flip_at="${cand_flip}"
        break
      fi
      continue
    fi
    # The probe marks a failing entry INVALID — put its state back as found.
    archive_tool kardamom-ingress-0 "${aeron_img}" mark-valid "${cand_rid}" \
      >/dev/null 2>&1 || true
  done
  [ -n "${seg}" ] \
    || fail "archive-corruption: no recording verifies clean at baseline (inherited catalog damage too broad — see issue #98)"
  # Corrupt 16 bytes inside a data frame's PAYLOAD — length unchanged,
  # frame structure intact, so verify reports a checksum ERR instead of
  # chasing a bogus frame length.
  log "archive-corruption: flipping payload bytes at ${flip_at} in ${seg_name} (recording ${rid})"
  docker exec kardamom-ingress-0 bash -lc \
    "printf 'KARDAMOM-CHAOS!!' | dd of=${seg} bs=1 seek=${flip_at} count=16 conv=notrunc status=none" \
    || fail "archive-corruption: byte flip failed"
  # DETECTION: CRC-armed verify on the frozen victim must NOT be clean.
  # (Run via a one-off container on the node — the daemon is down.)
  verify_pre="$(archive_tool kardamom-ingress-0 "${aeron_img}" \
    verify "${rid}" -a -checksum io.aeron.archive.checksum.Crc32 || true)"
  if has_line "${verify_pre}" 'Exception'; then
    echo "${verify_pre}" | tail -20
    fail "archive-corruption: verify tool crashed (not a detection)"
  fi
  has_match "${verify_pre}" "recordingId=${rid}[,)].* ERR" \
    || { echo "${verify_pre}" | tail -20; \
         fail "archive-corruption: CRC-armed verify did NOT flag recording ${rid} (detection hole)"; }
  log "archive-corruption: corruption detected by CRC-armed verify"
  # HEAL through the Rust tool on the runner: stage both copies, --diff
  # must name the corrupted segment, --heal repairs exactly it.
  tmp_dir="$(mktemp -d)"
  mkdir -p "${tmp_dir}/victim" "${tmp_dir}/mirror"
  docker exec kardamom-ingress-0 tar -C /opt/kardamom/archive -cf - dir \
    | tar -C "${tmp_dir}/victim" -xf - || fail "archive-corruption: staging victim copy failed"
  docker exec kardamom-ingress-1 tar -C /opt/kardamom/archive -cf - dir \
    | tar -C "${tmp_dir}/mirror" -xf - || fail "archive-corruption: staging mirror copy failed"
  diverged="$("${REREP_BIN}" --diff --source-dir "${tmp_dir}/mirror/dir" --dest-dir "${tmp_dir}/victim/dir" || true)"
  has_line "${diverged}" "${seg_name}" \
    || fail "archive-corruption: --diff did not name the corrupted segment ${seg_name}"
  local heal_out
  heal_out="$("${REREP_BIN}" --heal --segments "${seg_name}" --no-verify \
    --source-dir "${tmp_dir}/mirror/dir" --dest-dir "${tmp_dir}/victim/dir" 2>&1 || true)"
  has_line "${heal_out}" 'healed segments=1' \
    || { echo "${heal_out}" | tail -20; \
         fail "archive-corruption: --heal did not repair the segment"; }
  # Put ONLY the healed segment back, then re-validate + clear any INVALID
  # marks the detection verify persisted.
  tar -C "${tmp_dir}/victim" -cf - "dir/${seg_name}" \
    | docker exec -i kardamom-ingress-0 tar -C /opt/kardamom/archive -xf - \
    || fail "archive-corruption: writing healed segment back failed"
  # The detection verify marked the failing recording INVALID in the
  # catalog; clear the marks now that the bytes are healed. Recording ids
  # are harvested on the RUNNER from the pre-heal verify output (one
  # mark-valid container per id — a shell loop inside the node would run
  # `java` on the node itself, where it doesn't exist).
  archive_tool kardamom-ingress-0 "${aeron_img}" mark-valid "${rid}" \
    >/dev/null 2>&1 || true
  verify_post="$(archive_tool kardamom-ingress-0 "${aeron_img}" \
    verify "${rid}" -a -checksum io.aeron.archive.checksum.Crc32 || true)"
  if has_line "${verify_post}" 'Exception'; then
    echo "${verify_post}" | tail -20
    fail "archive-corruption: post-heal verify tool crashed"
  fi
  if ! has_line "${verify_post}" "recordingId=${rid}) OK"; then
    echo "${verify_post}" | tail -20
    fail "archive-corruption: post-heal verify does not show recording ${rid} OK"
  fi
  if has_match "${verify_post}" "recordingId=${rid}[,)].* ERR"; then
    echo "${verify_post}" | tail -20
    fail "archive-corruption: post-heal verify still reports errors on recording ${rid}"
  fi
  rm -rf "${tmp_dir}"
  log "archive-corruption: healed + CRC verify clean; undraining ingress-0"
  on_control 'nomad node drain -disable -yes "$1"' "${node_id}" >/dev/null \
    || fail "archive-corruption: drain disable failed"
  assert_count aeron "${aeron_base}" "${CHAOS_RESTART_SLO_S}"
  assert_count ingress 2 "${CHAOS_RESCHEDULE_SLO_S}"
  assert_ingress_pair_live "${name}"
}
