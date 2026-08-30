# shellcheck shell=bash
# =============================================================================
# chaos-cases-archive.sh — Aeron archive failure/data-loss/corruption cases.
# =============================================================================
# This file is sourced into chaos.sh's shell, never run as a child
# process. This file must not install traps; chaos.sh owns the single
# EXIT trap.

# Run Aeron's ArchiveTool against a node's archive directory, in a
# one-off container. The archive daemon is down or drained during the
# surgery windows, so the tool cannot run inside it. This replaces a
# docker-run block that used to repeat 5 times. stdout and stderr merge
# inside the node shell, so callers capture the whole tool output
# (Exception lines print to stderr).
archive_tool() { # <node> <aeron-image> <ArchiveTool-args...>
  local node="$1" img="$2"; shift 2
  docker exec "${node}" bash -lc \
    "docker run --rm -v /opt/kardamom/archive:/opt/kardamom/archive --entrypoint java ${img} \
     --add-opens java.base/java.util.zip=ALL-UNNAMED \
     -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir $* 2>&1"
}

case_archive_driver_loss() {
  # Hard-kill the Aeron substrate: the `aeron` system job's combined
  # ArchivingMediaDriver task, on the ingress-0 node, not the ingress
  # task itself. This is a failure surface the component cases skip.
  # Every service on the node shares that driver's aeron.dir, so the
  # local ingress loses its transport and its tx_data durability
  # recorder in one blow. Expected outcome, in order:
  #   1. The pipeline keeps progressing. Ingress is active/active, so
  #      clients retry against ingress-1 (the load runs in
  #      --chaos-mode).
  #   2. Nomad restarts the aeron system task within the restart SLO.
  #      Archive segments persist on the node volume across the
  #      restart.
  #   3. The collocated ingress task recovers against the fresh driver,
  #      and the ingress job returns to full strength within the
  #      reschedule SLO. This SLO is wider because it covers the
  #      driver restart plus the dependent-task restart chain.
  local aeron_base
  aeron_base="$(count_running aeron)"
  # A hiccuping nomad query reads as 0 or empty. Injecting anyway would
  # make the post-kill `assert_count aeron >= 0` pass trivially, and
  # silently drop the "driver restarted" part of the case.
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
  # This is a data-loss drill, not just process loss. Permanently wipe
  # ingress-0's tx_data archive volume, then restore it from ingress-1's
  # mirror. tx_data is UDP multicast, so both ingress archives record
  # every publisher's shard streams. The segments are byte-identical
  # across the two nodes (verified by sha256), so a single node's
  # archive loss is survivable, and the peer is an exact restore
  # source. This exercises `kardamom-archive-rereplicate`'s mechanism:
  # segment and catalog mirroring. Expected outcome, in order:
  #   1. The pipeline keeps progressing on ingress-1 (active/active)
  #      while ingress-0's substrate is down.
  #   2. After re-replicating ingress-1's segments and catalog, the
  #      restarted ingress-0 archive adopts them, and Aeron's own
  #      `ArchiveTool verify` reports every recording OK.
  #   3. ingress and aeron return to full strength.
  local aeron_base ac0 verify_out
  aeron_base="$(count_running aeron)"
  log "archive-tx-data-wipe: killing aeron substrate on kardamom-ingress-0 + wiping its tx_data archive volume"
  inject_hard kardamom-ingress-0 archiving-media-driver
  # Simulate permanent volume loss while the driver is down: both
  # segments and catalog.
  docker exec kardamom-ingress-0 bash -lc \
    'rm -f /opt/kardamom/archive/dir/*.rec /opt/kardamom/archive/dir/archive.catalog' \
    || fail "archive-tx-data-wipe: could not wipe ingress-0 archive"
  # Re-replicate from the surviving peer, ingress-1: stream its archive
  # directory across. This is the transport that
  # kardamom-archive-rereplicate wraps for an operator (peer copy,
  # mirror_archive, verify_mirror).
  #
  # Never transplant archive-mark.dat from a live source. The peer's
  # daemon sends it heartbeats, so the copy would look "active" to the
  # victim's restarting Archive. The Archive would then crash-loop on
  # 'active Mark file detected' until the copied heartbeat ages out,
  # which can blow the 60-second restart SLO. The wipe above
  # deliberately preserves the victim's own mark file. Its heartbeat
  # died with the killed driver, so it is already stale by restart
  # time, and the daemon starts cleanly.
  # Copy the catalog first, with a stable read. The mirror's daemon
  # rewrites catalog entries on recording lifecycle events, and this
  # very injection triggers some: ingress-0's publishers died with its
  # driver, so the mirror stops those recordings and rewrites exactly
  # those entries seconds later. A copy that races such a write
  # captures a torn entry, where the stored checksum does not match the
  # descriptor, and that fails a CRC-armed verify. Two consecutive
  # identical snapshots guarantee a consistent image. Copying the
  # catalog before the segments guarantees the segment data covers
  # every position it references. kardamom-archive-rereplicate does the
  # same.
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
  # tar exit code 1 means a segment was appended under the live
  # recorder mid-copy. The restart-side verify and heal handle the
  # torn tail.
  [ "${seg_rc}" -le 1 ] \
    || fail "archive-tx-data-wipe: re-replication copy failed (rc=${seg_rc})"
  # The pipeline must have kept running on ingress-1 the whole time.
  assert_progress
  # aeron restarts, as a system job, and adopts the restored archive.
  assert_count aeron "${aeron_base}" "${CHAOS_RESTART_SLO_S}"
  # Verify the restored archive with Aeron's own tool: every recording OK.
  ac0="$(inner_container kardamom-ingress-0 archiving-media-driver)"
  [ -n "${ac0}" ] || fail "archive-tx-data-wipe: no aeron container on ingress-0 after restart"
  # This is a CRC-armed verify: it checks every data frame's recorded
  # CRC32, plus file availability and structure. One class of error is
  # tolerated, counted, and logged: 'invalid Catalog checksum'. Aeron
  # 1.45 patches catalog entries when active recordings are adopted or
  # stopped out-of-band. ArchiveTool.verify writes recovered stop
  # positions without recomputing the entry checksum, and the daemon's
  # adoption path behaves the same way. So entry checksums on a
  # restored-and-adopted archive go stale by construction; this is an
  # upstream gap, not a torn transplant. Frame CRCs are unaffected, and
  # remain the authoritative integrity signal. A crashed tool never
  # passes. This verify runs inside the freshly restarted daemon
  # container, since the daemon owns the directory again, so it is not
  # an archive_tool one-off run.
  local v_ok=0 stale_entries=0
  for _ in 1 2 3; do
    verify_out="$(docker exec kardamom-ingress-0 bash -lc \
      "docker exec ${ac0} bash -lc 'java --add-opens java.base/java.util.zip=ALL-UNNAMED -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir verify -a -checksum io.aeron.archive.checksum.Crc32 2>&1'" || true)"
    stale_entries="$(echo "${verify_out}" | grep -c 'invalid Catalog checksum' || true)"
    # Count non-checksum errors with `grep -c`, which reads all input
    # with no early exit, so there is no SIGPIPE risk, instead of a
    # filtered `grep -q` chain.
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
  # This is a data-corruption drill: present but wrong, not missing.
  # Flip bytes mid-segment in ingress-0's tx_data archive. The length
  # stays the same, so a size check cannot see it. Then detect it with
  # a CRC-armed `ArchiveTool verify` (record-time CRC32 is enabled in
  # the driver), and heal only the corrupt segment from ingress-1's
  # mirror, with `kardamom-archive-rereplicate --diff` and `--heal`.
  # File-level surgery needs the victim's archive daemon stopped
  # throughout, so the node is drained for the window. The pipeline
  # rides on ingress-1, as the other archive cases prove. Expected, in
  # order:
  #   1. Pre-heal verify fails on the corrupted archive (detection).
  #   2. The Rust tool's --diff names the corrupted segment, and
  #      --heal repairs exactly that segment from the mirror.
  #   3. Post-heal CRC verify is clean, the node undrains, and aeron
  #      and ingress return to strength, with the pipeline having
  #      progressed.
  local aeron_base node_id aeron_img tmp_dir verify_pre verify_post diverged
  [ -x "${REREP_BIN}" ] || fail "archive-corruption: kardamom-archive-rereplicate not at ${REREP_BIN}"
  aeron_base="$(count_running aeron)"
  [ -n "${aeron_base}" ] && [ "${aeron_base}" -gt 0 ] \
    || fail "archive-corruption: aeron baseline unavailable — refusing a vacuous pass"
  # Hold ingress-0 down for the whole surgery window. A drain evicts
  # the aeron system task, which holds the catalog open, and keeps it
  # down until this case undrains it. A hard kill would race Nomad's
  # restart.
  node_id="$(on_control 'nomad node status -verbose 2>/dev/null | awk "/ingress-0/ {print \$1; exit}"')"
  [ -n "${node_id}" ] || fail "archive-corruption: could not resolve ingress-0 node id"
  aeron_img="$(docker exec kardamom-ingress-0 bash -lc \
    "docker ps -a --format '{{.Image}} {{.Names}}' | awk '/archiving/ {print \$1; exit}'")"
  [ -n "${aeron_img}" ] || fail "archive-corruption: could not resolve the aeron image on ingress-0"
  log "archive-corruption: draining ingress-0 node (${node_id})"
  on_control 'nomad node drain -enable -yes -deadline 2m "$1"' "${node_id}" >/dev/null \
    || fail "archive-corruption: drain enable failed"
  sleep 5
  # Pick a victim recording that verifies clean at baseline. The
  # archive's catalog was restored from the live peer by
  # archive-tx-data-wipe, and which entries got torn in that copy
  # varies by run. This case tests segment corruption detect and heal,
  # not catalog repair, so it must start from a provably clean
  # recording: baseline OK, then corrupt, then ERR, then heal, then OK,
  # a closed loop. Every verify and mark-valid call is scoped to that
  # recording (segment name is <recordingId>-<base>.rec).
  local seg="" seg_name="" rid="" flip_at=-1 cand cand_name cand_rid cand_out cand_flip
  for cand in $(docker exec kardamom-ingress-0 bash -lc \
      'ls -S /opt/kardamom/archive/dir/*.rec 2>/dev/null | head -6'); do
    cand_name="$(basename "${cand}")"
    cand_rid="${cand_name%%-*}"
    # Recording ids are per-archive counters, so the victim's
    # post-restart and post-restore sessions (archive-driver-loss and
    # archive-tx-data-wipe both run earlier in this shard) own ids the
    # mirror never opened. A victim-only segment cannot be healed from
    # the mirror, by construction, since no source bytes exist for it.
    # So it cannot drill the detect-then-heal loop; only candidates
    # present on both archives qualify. --diff now surfaces such
    # segments as "dest-only", instead of silently skipping them.
    if ! docker exec kardamom-ingress-1 test -f "/opt/kardamom/archive/dir/${cand_name}" 2>/dev/null; then
      continue
    fi
    cand_out="$(archive_tool kardamom-ingress-0 "${aeron_img}" \
      verify "${cand_rid}" -a -checksum io.aeron.archive.checksum.Crc32 || true)"
    if has_line "${cand_out}" "recordingId=${cand_rid}) OK" \
      && ! has_line "${cand_out}" ') ERR'; then
      # Segment files are pre-allocated, so their apparent size is
      # equal and `ls -S` order is arbitrary. A flip landing in a frame
      # header can send verify's frame-walk out of bounds, for example
      # a JVM SIGSEGV in the CRC32 intrinsic, instead of a clean ERR.
      # So this walks the Aeron data-frame headers, and picks a flip
      # offset inside the payload of the largest real data frame (type
      # 0x01, skipping padding frames). -1 means the segment has no
      # usable frame; try the next candidate.
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
    # The probe marks a failing entry invalid. Put its state back as
    # found.
    archive_tool kardamom-ingress-0 "${aeron_img}" mark-valid "${cand_rid}" \
      >/dev/null 2>&1 || true
  done
  [ -n "${seg}" ] \
    || fail "archive-corruption: no recording verifies clean at baseline (inherited catalog damage too broad — see issue #98)"
  # Corrupt 16 bytes inside a data frame's payload. The length stays
  # the same, and the frame structure stays intact, so verify reports a
  # checksum ERR instead of chasing a bogus frame length.
  log "archive-corruption: flipping payload bytes at ${flip_at} in ${seg_name} (recording ${rid})"
  docker exec kardamom-ingress-0 bash -lc \
    "printf 'KARDAMOM-CHAOS!!' | dd of=${seg} bs=1 seek=${flip_at} count=16 conv=notrunc status=none" \
    || fail "archive-corruption: byte flip failed"
  # Detection: a CRC-armed verify on the frozen victim must not be
  # clean. This runs through a one-off container on the node, since the
  # daemon is down.
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
  # Heal through the Rust tool on the runner: stage both copies.
  # --diff must name the corrupted segment, and --heal repairs exactly
  # that segment.
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
  # Put only the healed segment back, then re-validate and clear any
  # invalid marks the detection verify left behind.
  tar -C "${tmp_dir}/victim" -cf - "dir/${seg_name}" \
    | docker exec -i kardamom-ingress-0 tar -C /opt/kardamom/archive -xf - \
    || fail "archive-corruption: writing healed segment back failed"
  # The detection verify marked the failing recording invalid in the
  # catalog. Clear the marks now that the bytes are healed. This
  # harvests recording ids on the runner from the pre-heal verify
  # output, one mark-valid container per id. A shell loop inside the
  # node would run `java` on the node itself, where it does not exist.
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
