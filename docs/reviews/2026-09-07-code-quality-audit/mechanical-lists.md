# Mechanical lists

These tables come from tooling, not from reading. Sources: clippy `too_many_lines` at threshold 50 (code lines, comments and blanks excluded), a code-line counter for file size (blank and comment-only lines excluded), the `unreachable_pub` rustc lint, and clippy with `--force-warn` for lints hidden by `allow` attributes.


### R2 per crate (production / test)

| crate | production fns > 50 lines | test fns > 50 lines |
|---|---|---|
| batcher | 7 | 11 |
| bench | 21 | 5 |
| cluster-adapter | 3 | 0 |
| cluster-client | 1 | 0 |
| da_watcher | 6 | 0 |
| deployer | 1 | 1 |
| e2e | 6 | 22 |
| engine | 10 | 18 |
| exec-core | 10 | 8 |
| executor | 3 | 6 |
| footprint | 3 | 0 |
| ingress | 2 | 9 |
| log | 4 | 3 |
| obs | 1 | 1 |
| reconstruct | 1 | 3 |
| sequencer | 4 | 4 |
| state | 9 | 8 |
| stm | 12 | 7 |
| types | 1 | 0 |
| validator | 12 | 9 |

### R2 production functions over 100 code lines

| code lines | function | location |
|---|---|---|
| 618 | `main` | `crates/bench/src/bin/stm-p2.rs:1308` |
| 458 | `block_tail` | `crates/stm/src/execute.rs:3295` |
| 407 | `run_mdbx_ab` | `crates/bench/src/bin/stm-p2.rs:825` |
| 373 | `run_pipelined` | `crates/bench/src/bin/stm-p2.rs:362` |
| 347 | `main` | `crates/validator/src/bin/kardamom-validator/main.rs:69` |
| 252 | `with_pool` | `crates/stm/src/execute.rs:1877` |
| 242 | `push_prepared` | `crates/stm/src/execute.rs:2803` |
| 216 | `run` | `crates/bench/src/load/mod.rs:101` |
| 205 | `run_worker_block` | `crates/stm/src/execute.rs:3980` |
| 196 | `main` | `crates/bench/src/bin/stm-p0.rs:126` |
| 188 | `generate` | `crates/bench/src/stm/uniswap.rs:115` |
| 170 | `spawn_tx_ordering_reader` | `crates/engine/src/reader.rs:480` |
| 158 | `main` | `crates/da_watcher/src/bin/kardamom-da-watcher.rs:227` |
| 156 | `launch_with_l1` | `crates/e2e/src/harness/mod.rs:264` |
| 146 | `execute_one` | `crates/stm/src/execute.rs:4421` |
| 142 | `apply` | `crates/state/src/writer/mod.rs:238` |
| 141 | `begin_block_deferred_inner` | `crates/stm/src/execute.rs:2296` |
| 138 | `main` | `crates/executor/src/bin/kardamom-executor/main.rs:47` |
| 123 | `on_boundary` | `crates/engine/src/actor/exec_thread.rs:674` |
| 122 | `main` | `crates/sequencer/src/bin/kardamom-sequencer/main.rs:172` |
| 117 | `run_replay_merge` | `crates/log/src/replay.rs:241` |
| 113 | `evaluate` | `crates/bench/src/load/accounting.rs:136` |
| 111 | `watch_and_challenge` | `crates/batcher/src/optimistic.rs:138` |
| 103 | `run` | `crates/batcher/src/live.rs:477` |
| 102 | `deep_compare` | `crates/state/src/integrity/compare.rs:24` |
| 101 | `launch` | `crates/e2e/src/harness/sealer.rs:53` |

### R2 production functions 51-100 code lines

| code lines | function | location |
|---|---|---|
| 100 | `execute_tx_decoded` | `crates/exec-core/src/executor/scope.rs:419` |
| 96 | `launch` | `crates/e2e/src/harness/l1/mod.rs:65` |
| 94 | `main` | `crates/ingress/src/bin/kardamom-ingress/main.rs:166` |
| 93 | `fetch_latest_checkpoint` | `crates/state/src/checkpoint_transfer.rs:217` |
| 92 | `verify_witness_anchored` | `crates/exec-core/src/anchor/mod.rs:175` |
| 92 | `spawn` | `crates/da_watcher/src/interop/watcher.rs:178` |
| 90 | `run_one` | `crates/executor/src/parallel.rs:186` |
| 85 | `write_summary` | `crates/bench/src/perf/report.rs:106` |
| 84 | `run_bal_publisher` | `crates/executor/src/bal.rs:112` |
| 84 | `post_confirmed` | `crates/batcher/src/live.rs:254` |
| 83 | `anchor_block_witness` | `crates/validator/src/witness.rs:63` |
| 83 | `ramp_to_max` | `crates/bench/src/load/mod.rs:366` |
| 82 | `main` | `crates/deployer/src/main.rs:122` |
| 82 | `run_capture` | `crates/bench/src/stm/capture.rs:21` |
| 82 | `pregenerate_family` | `crates/bench/src/load/defi.rs:333` |
| 82 | `main` | `crates/batcher/src/bin/kardamom-batcher.rs:177` |
| 81 | `drive_block` | `crates/engine/src/replay.rs:174` |
| 79 | `recompute_post_root` | `crates/exec-core/src/anchor/mod.rs:306` |
| 78 | `new` | `crates/stm/src/pool.rs:110` |
| 78 | `seed_genesis` | `crates/state/src/genesis.rs:81` |
| 78 | `grade_block` | `crates/footprint/src/grade.rs:87` |
| 78 | `execute_deposit` | `crates/exec-core/src/executor/scope.rs:136` |
| 78 | `insert_in` | `crates/exec-core/src/anchor/sparse.rs:172` |
| 78 | `run` | `crates/bench/src/bin/perf.rs:185` |
| 77 | `basic_inner` | `crates/stm/src/execute.rs:275` |
| 76 | `execute_xchain_tx` | `crates/exec-core/src/executor/xchain.rs:53` |
| 75 | `snapshot` | `crates/bench/src/load/scrape.rs:117` |
| 74 | `main` | `crates/state/src/bin/kardamom-statecheck.rs:60` |
| 74 | `fetch_tx_data` | `crates/log/src/refetch.rs:133` |
| 74 | `execute_deposit_tx` | `crates/exec-core/src/executor/deposit.rs:43` |
| 74 | `run_case` | `crates/e2e/src/bin/kardamom-semantics.rs:152` |
| 74 | `decode_relayed_payload` | `crates/cluster-adapter/src/wire/egress.rs:106` |
| 74 | `main` | `crates/batcher/src/bin/kardamom-archive-rereplicate.rs:66` |
| 73 | `analyze` | `crates/footprint/src/oracle.rs:152` |
| 73 | `process_block` | `crates/engine/src/shadow.rs:112` |
| 73 | `up` | `crates/bench/src/perf/cluster.rs:108` |
| 73 | `print_report` | `crates/bench/src/load/accounting.rs:313` |
| 72 | `check_receipts_index` | `crates/state/src/integrity/checks.rs:153` |
| 72 | `remove_in` | `crates/exec-core/src/anchor/sparse.rs:275` |
| 71 | `resolve_recording` | `crates/log/src/replay.rs:441` |
| 70 | `describe` | `crates/engine/src/metrics.rs:71` |
| 70 | `process_once` | `crates/da_watcher/src/watcher.rs:95` |
| 70 | `?` | `crates/da_watcher/src/interop/source.rs:228` |
| 70 | `resolve_paths` | `crates/da_watcher/src/bin/kardamom-da-watcher.rs:144` |
| 70 | `prepare` | `crates/bench/src/workflows/mixed.rs:87` |
| 68 | `decode_message_sent` | `crates/validator/src/interop/extract.rs:189` |
| 68 | `spawn_bal_pump` | `crates/validator/src/bin/kardamom-validator/pumps.rs:32` |
| 68 | `bootstrap_trie_from_state` | `crates/state/src/recovery/mod.rs:125` |
| 68 | `handle_cmd` | `crates/log/src/aeron_live/thread.rs:163` |
| 67 | `summary` | `crates/footprint/src/oracle.rs:243` |
| 66 | `on_tx` | `crates/engine/src/actor/exec_thread.rs:386` |
| 65 | `prune` | `crates/stm/src/execute.rs:1487` |
| 65 | `init` | `crates/obs/src/lib.rs:64` |
| 65 | `join_envelope` | `crates/engine/src/reader.rs:733` |
| 64 | `ingest` | `crates/engine/src/reader/cluster/mod.rs:140` |
| 62 | `replay_unavailable_fallback` | `crates/engine/src/bin_support.rs:396` |
| 61 | `execute_block_parallel_scoped` | `crates/validator/src/parallel/engine.rs:363` |
| 61 | `record_writeset_into_bal_inner` | `crates/exec-core/src/executor/write_set.rs:115` |
| 60 | `connect_inner` | `crates/cluster-adapter/src/live/mod.rs:260` |
| 60 | `deploy_and_confirm` | `crates/bench/src/load/defi.rs:210` |
| 59 | `spawn` | `crates/validator/src/epoch_verify.rs:249` |
| 59 | `resync_tick` | `crates/sequencer/src/sequencer.rs:236` |
| 59 | `receipt_feed_task` | `crates/bench/src/load/feed.rs:19` |
| 59 | `shadow_replay` | `crates/bench/src/bin/stm-p0.rs:64` |
| 59 | `dispatch` | `crates/bench/src/benchmark.rs:192` |
| 58 | `execute_block_parallel` | `crates/validator/src/parallel/engine.rs:253` |
| 58 | `spawn_attester` | `crates/validator/src/attester.rs:494` |
| 57 | `spawn_prover_spool` | `crates/validator/src/prover.rs:190` |
| 57 | `run` | `crates/stm/src/pool.rs:210` |
| 57 | `flush_admit_batch` | `crates/stm/src/execute.rs:2695` |
| 56 | `derive_remote_epoch` | `crates/types/src/xchain.rs:414` |
| 56 | `run_egress_watermark_feed` | `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:63` |
| 56 | `launch` | `crates/e2e/src/harness/aeron.rs:51` |
| 56 | `main` | `crates/bench/src/bin/load.rs:179` |
| 55 | `advance_base` | `crates/stm/src/execute.rs:2541` |
| 55 | `execute_xchain` | `crates/exec-core/src/executor/scope.rs:263` |
| 55 | `on_session_event` | `crates/cluster-client/src/session/mod.rs:266` |
| 55 | `decode_egress` | `crates/cluster-adapter/src/wire/egress.rs:44` |
| 55 | `submit_next_proof` | `crates/batcher/src/prover_submit.rs:48` |
| 54 | `from_alloy` | `crates/validator/src/parallel/claims.rs:67` |
| 54 | `main` | `crates/reconstruct/src/bin/kardamom-reconstruct.rs:64` |
| 54 | `process_once` | `crates/da_watcher/src/interop/watcher.rs:89` |
| 53 | `execute_batch` | `crates/validator/src/parallel/engine.rs:142` |
| 53 | `process` | `crates/sequencer/src/state/mod.rs:83` |
| 53 | `claim_next_batch` | `crates/batcher/src/optimistic.rs:59` |
| 52 | `records_json` | `crates/validator/src/parallel/dump.rs:15` |
| 52 | `check_headers` | `crates/state/src/integrity/checks.rs:93` |
| 52 | `receipt_to_rpc` | `crates/ingress/src/json_rpc.rs:268` |
| 52 | `spawn_commit` | `crates/engine/src/actor/commit_thread.rs:59` |
| 51 | `read_response_head` | `crates/state/src/checkpoint_transfer.rs:376` |
| 51 | `spawn_executor_at` | `crates/e2e/src/harness/services.rs:261` |

### R2 test functions over 50 code lines

| code lines | function | location |
|---|---|---|
| 236 | `capture_anchor_guest_and_live_trie_agree` | `crates/validator/tests/witness_anchoring.rs:92` |
| 236 | `optimistic_claim_finalize_and_challenge_paths` | `crates/batcher/tests/optimistic_e2e.rs:78` |
| 223 | `delivery` | `crates/e2e/src/scenarios/xchain.rs:166` |
| 210 | `spooled_frame_reverifies_and_matches_the_live_writer_root` | `crates/validator/tests/prover_spool.rs:76` |
| 190 | `real_groth16_proof_accepted_on_chain_challenge_and_grief_rejected` | `crates/batcher/tests/optimistic_proof_e2e.rs:92` |
| 173 | `defi_execution_allocation_profile` | `crates/bench/tests/alloc_profile.rs:43` |
| 172 | `finalize_withdrawal` | `crates/e2e/src/scenarios/bridge.rs:224` |
| 158 | `posted_batch_proof_advances_the_oracle_root_chain` | `crates/batcher/tests/proof_submission_e2e.rs:57` |
| 151 | `m4_canonical_b_order_drives_receipts` | `crates/executor/tests/m_plus_one_join.rs:173` |
| 135 | `m_eq_4_sequencers_publish_canonical_refs` | `crates/sequencer/tests/multi_sequencer_dual_write.rs:83` |
| 133 | `rebuild_from_l1_reconstructs_canonical_state_root` | `crates/reconstruct/tests/reconstruct_l1_e2e.rs:69` |
| 133 | `k20_defi_parallel_matches_sequential_across_compositions` | `crates/bench/tests/parallel_defi_repro.rs:70` |
| 130 | `blob_roundtrip_executes_remote_epochs` | `crates/reconstruct/src/lib.rs:221` |
| 129 | `run` | `crates/e2e/src/scenarios/nonce_gap.rs:50` |
| 129 | `live_sender_confirms_and_rejects_foreign_writer` | `crates/batcher/tests/anvil_e2e.rs:157` |
| 128 | `forward_leg` | `crates/e2e/src/scenarios/xchain_two_stacks.rs:198` |
| 128 | `run` | `crates/e2e/src/scenarios/consistency.rs:72` |
| 126 | `sharded_admission_byte_identical` | `crates/stm/tests/equivalence.rs:1228` |
| 126 | `run_case` | `crates/exec-core/tests/eest_state.rs:168` |
| 124 | `collect_canonical_blocks` | `crates/e2e/src/scenarios/xchain_da_parity.rs:70` |
| 122 | `mv_as_layer_pipeline_wound_aborts_and_recovers` | `crates/stm/tests/equivalence.rs:953` |
| 116 | `speculative_pipeline_wound_aborts_and_recovers` | `crates/stm/tests/equivalence.rs:772` |
| 116 | `eest_state_tests_conform` | `crates/exec-core/tests/eest_state.rs:323` |
| 115 | `actor_receipts_match_naive_reference` | `crates/executor/tests/diff_reference.rs:184` |
| 115 | `section6_conformance_m_plus_one_to_l1_and_back` | `crates/batcher/tests/section6_conformance.rs:197` |
| 112 | `full_withdrawal_finalize_and_challenge` | `crates/validator/tests/withdrawal_e2e.rs:241` |
| 110 | `verified_l1_endpoint` | `crates/e2e/src/scenarios/divergence.rs:206` |
| 109 | `tx_ref_arriving_before_envelope_still_joins` | `crates/executor/tests/m_plus_one_join.rs:376` |
| 104 | `write_synthetic_archives` | `crates/batcher/tests/docker_e2e.rs:61` |
| 102 | `streaming_release_and_wound_correction` | `crates/stm/tests/equivalence.rs:650` |
| 99 | `replay_10_txs_across_3_blocks_yields_expected_c_stream` | `crates/executor/tests/replay_integration.rs:124` |
| 98 | `run_e2e_throughput` | `crates/e2e/benches/e2e_throughput.rs:25` |
| 97 | `incremental_equals_full_rebuild_over_random_blocks` | `crates/state/src/trie/incremental_tests.rs:181` |
| 95 | `whole_block_strategy_receives_buffered_xchain_records` | `crates/engine/src/actor/exec_tests.rs:741` |
| 94 | `bag_scheduler_byte_identical` | `crates/stm/tests/equivalence.rs:1091` |
| 94 | `multi_l2_deploy_and_atomic_upgrade` | `crates/deployer/tests/deploy_e2e.rs:97` |
| 90 | `ingress_stage_costs` | `crates/ingress/tests/stage_costs.rs:32` |
| 89 | `old_and_new_deposit_paths_agree` | `crates/exec-core/src/executor/deposit.rs:297` |
| 89 | `defi_workload_executes_on_the_engine` | `crates/bench/tests/defi_on_engine.rs:35` |
| 85 | `s13_xchain_da_parity` | `crates/e2e/tests/chain_semantics/xchain.rs:161` |
| 84 | `update_for_block` | `crates/state/src/trie/mod.rs:187` |
| 84 | `write_archives` | `crates/batcher/tests/section6_conformance.rs:60` |
| 82 | `exec_pipelines_commit_and_next_block_reads_parent_layer` | `crates/engine/src/actor/exec_pipeline_tests.rs:28` |
| 81 | `extension_collapse_regrow_no_stale_orphans` | `crates/state/src/trie/incremental_tests.rs:321` |
| 80 | `run` | `crates/e2e/src/scenarios/rpc_liveness.rs:91` |
| 76 | `stm_strategy_matches_sequential_capture_byte_for_byte` | `crates/executor/tests/stm_block_exec_ab.rs:82` |
| 76 | `bench_actor_throughput` | `crates/executor/benches/sequential_throughput.rs:229` |
| 74 | `every_witness_lie_is_refuted` | `crates/exec-core/tests/anchor_state.rs:308` |
| 74 | `l1_batch` | `crates/e2e/src/scenarios/l1_batch.rs:30` |
| 73 | `b_reader_joins_against_a_buffer_in_canonical_order` | `crates/log/tests/testing_fakes.rs:151` |
| 71 | `setup` | `crates/validator/tests/withdrawal_e2e.rs:88` |
| 71 | `incremental_block_on_bootstrapped_trie_matches_oracle` | `crates/state/src/recovery/tests.rs:178` |
| 71 | `back_pressured_publish_does_not_starve_a_live_subscription` | `crates/log/tests/offer_starvation.rs:77` |
| 71 | `exec_runs_two_txs_and_emits_slim_boundary` | `crates/engine/src/actor/exec_tests.rs:21` |
| 71 | `deploy_settlement_and_post_batch_emits_event` | `crates/batcher/tests/anvil_e2e.rs:62` |
| 70 | `run` | `crates/e2e/src/scenarios/nonce_unordered.rs:40` |
| 69 | `stateless_replay_reproduces_recorded_execution` | `crates/validator/tests/stateless_reexec.rs:168` |
| 69 | `integration_1000_txs_100_senders_with_chaos` | `crates/sequencer/tests/sequencer_integration.rs:69` |
| 69 | `callback_leg` | `crates/e2e/src/scenarios/xchain_two_stacks.rs:357` |
| 68 | `one_hundred_txs_route_and_receive_receipts` | `crates/ingress/tests/end_to_end_test.rs:26` |
| 68 | `channel_b_reader_emits_tx_and_boundary_in_canonical_order` | `crates/engine/src/reader.rs:930` |
| 68 | `deposit_credit_is_visible_to_later_txs_in_the_block` | `crates/engine/src/actor/exec_tests.rs:119` |
| 68 | `run_workload` | `crates/e2e/src/scenarios/da_parity.rs:75` |
| 67 | `peek_nonce_matches_full_decode` | `crates/sequencer/src/nonce_decode.rs:113` |
| 67 | `bench_throughput` | `crates/ingress/benches/throughput.rs:47` |
| 67 | `activates_at_timestamp` | `crates/e2e/src/scenarios/upgrade.rs:234` |
| 67 | `happy_path_in_order_resolution` | `crates/batcher/tests/multi_archive_reader.rs:69` |
| 66 | `exec_hands_off_shadow_captures_at_boundary` | `crates/engine/src/actor/exec_tests.rs:324` |
| 65 | `channel_b_reader_expands_an_epoch_into_marker_plus_deposits` | `crates/engine/src/reader.rs:1007` |
| 65 | `assert_reconstructed_interop_state` | `crates/e2e/src/scenarios/xchain_da_parity.rs:251` |
| 64 | `bootstrap_builds_trie_matching_oracle_on_trie_off_state` | `crates/state/src/recovery/tests.rs:15` |
| 64 | `blob_roundtrip_reconstructs_identical_state_root` | `crates/reconstruct/src/lib.rs:136` |
| 64 | `build_substitutions` | `crates/e2e/src/scenarios/rpc_vectors.rs:106` |
| 63 | `sequencer_core_loop_allocation_profile` | `crates/sequencer/tests/alloc_profile.rs:90` |
| 63 | `channel_b_reader_expands_a_remote_epoch_into_marker_plus_messages` | `crates/engine/src/reader.rs:1136` |
| 63 | `gap_halts_pair_not_chain` | `crates/e2e/src/scenarios/xchain.rs:431` |
| 63 | `create_then_call_across_chunks_in_one_block` | `crates/bench/tests/parallel_defi_repro.rs:236` |
| 63 | `ingress_submission_allocation_profile` | `crates/bench/tests/alloc_profile_ingress.rs:142` |
| 62 | `mds_duplicate_receipts_dedup_resolves_submit_once` | `crates/ingress/tests/end_to_end_test.rs:191` |
| 60 | `bench_e2e_latency` | `crates/ingress/benches/latency.rs:44` |
| 60 | `corrupt_bal_halts_validator` | `crates/e2e/src/scenarios/divergence.rs:30` |
| 59 | `each_tx_lands_on_keccak_partition` | `crates/ingress/tests/routing_test.rs:20` |
| 58 | `trie_writer_root_matches_model_and_persists` | `crates/state/src/writer/tests.rs:59` |
| 58 | `bootstrap_corrects_genesis_stale_mirror_under_newer_state` | `crates/state/src/recovery/tests.rs:101` |
| 58 | `nonce_too_low_skips_with_marker_receipt_and_chain_continues` | `crates/exec-core/src/executor/scope.rs:781` |
| 58 | `remote_epoch_messages_execute_as_0x7d_receipts` | `crates/engine/src/actor/exec_tests.rs:667` |
| 58 | `resume_after_empty_block_backlog` | `crates/engine/src/actor/exec_resume_tests.rs:107` |
| 58 | `resume_executes_from_cursor_with_absolute_counts` | `crates/engine/src/actor/exec_resume_tests.rs:27` |
| 57 | `run_pipeline` | `crates/validator/tests/forged_envelope_chaos.rs:138` |
| 57 | `pooled_dispatch_matches_scoped_reference` | `crates/validator/src/parallel/engine_tests.rs:702` |
| 57 | `init_and_scrape_on_current_thread_runtime` | `crates/obs/tests/init_without_runtime.rs:21` |
| 57 | `racing_replica_rejection_is_overridden_by_twin_success` | `crates/ingress/tests/end_to_end_test.rs:285` |
| 57 | `initiate_withdrawal` | `crates/e2e/src/scenarios/bridge.rs:135` |
| 56 | `exec_record` | `crates/validator/src/parallel/engine_tests.rs:116` |
| 56 | `aeron_live_send_friendly_round_trip` | `crates/log/tests/aeron_live_e2e.rs:40` |
| 56 | `run` | `crates/engine/src/actor.rs:112` |
| 56 | `out_of_order_b_refs_a_positions_still_resolve` | `crates/batcher/tests/multi_archive_reader.rs:155` |
| 55 | `exec_settles_inflight_commits_while_idle` | `crates/engine/src/actor/exec_pipeline_tests.rs:204` |
| 54 | `hot_chain_streams_through_the_fifo` | `crates/stm/tests/equivalence.rs:562` |
| 54 | `proxy_parks_until_watermark_advances` | `crates/ingress/tests/end_to_end_test.rs:119` |
| 54 | `encoding_is_injective_across_compactions` | `crates/exec-core/tests/write_set_encoding.rs:61` |
| 53 | `honest_witness_verifies_and_recomputes_the_oracle_post_root` | `crates/exec-core/tests/anchor_state.rs:237` |
| 53 | `reader_joins_two_sessions_at_same_position` | `crates/engine/src/reader.rs:1402` |
| 53 | `an_active_feature_beats_once_per_block` | `crates/engine/src/actor/exec_tests.rs:475` |
| 53 | `aeron_cluster_starts_and_batcher_round_trips_the_m_plus_one_topology` | `crates/batcher/tests/docker_e2e.rs:179` |
| 52 | `receipt_mismatch_dumps_flight_ring` | `crates/validator/src/seams.rs:424` |
| 52 | `cheap_blocks_are_declined_and_still_match` | `crates/stm/tests/equivalence.rs:447` |
| 52 | `four_readers_with_distinct_snapshots` | `crates/state/tests/concurrent_readers.rs:17` |
| 52 | `exec_handoff_carries_a_populated_bal` | `crates/engine/src/actor/exec_tests.rs:203` |
| 52 | `exec_pipelines_k_deep_and_blocks_only_at_capacity` | `crates/engine/src/actor/exec_pipeline_tests.rs:134` |
| 52 | `s14_xchain_two_stacks` | `crates/e2e/tests/chain_semantics/xchain.rs:80` |
| 52 | `forged_epoch_halts_validator` | `crates/e2e/src/scenarios/divergence.rs:121` |
| 51 | `subscription_streams_deduped_and_filtered_receipts` | `crates/ingress/tests/receipt_subscription_test.rs:136` |
| 51 | `witness` | `crates/exec-core/tests/anchor_state.rs:157` |
| 51 | `activation_is_judged_against_the_blocks_own_header_timestamp` | `crates/engine/src/actor/exec_tests.rs:541` |

### R3 files over 500 code lines

| code lines | raw lines | file |
|---|---|---|
| 3153 | 4620 | `crates/stm/src/execute.rs` |
| 1679 | 1993 | `crates/bench/src/bin/stm-p2.rs` |
| 1154 | 1372 | `crates/stm/tests/equivalence.rs` |
| 1032 | 1466 | `crates/engine/src/reader.rs` |
| 809 | 1102 | `crates/exec-core/src/executor/scope.rs` |
| 750 | 893 | `crates/engine/src/actor/exec_tests.rs` |
| 657 | 814 | `crates/validator/src/parallel/engine_tests.rs` |
| 629 | 937 | `crates/engine/src/actor/exec_thread.rs` |
| 605 | 813 | `crates/validator/src/epoch_verify.rs` |
| 569 | 845 | `crates/e2e/src/harness/mod.rs` |
| 555 | 848 | `crates/validator/src/attester.rs` |
| 541 | 805 | `crates/types/src/xchain.rs` |
| 518 | 724 | `crates/da_watcher/src/interop/watcher.rs` |

### R8 compiler pass: unreachable `pub` items

| location | item |
|---|---|
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:26` | `pub type RecorderReady = oneshot::Receiver<(u8, Result<i64, String>)>;` |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:38` | `pub fn spawn_tx_data_recorders(` |
| `crates/ingress/src/bin/kardamom-ingress/recorders.rs:85` | `pub async fn wait_for_recorders(ready: Vec<RecorderReady>) -> Result<()> {` |
| `crates/ingress/src/json_rpc.rs:385` | `pub struct PeerAddrLayer;` |
| `crates/ingress/src/json_rpc.rs:395` | `pub struct PeerAddrService<S> {` |
| `crates/ingress/tests/common/mod.rs:15` | `pub fn sign_legacy_tx(signer: &PrivateKeySigner, nonce: u64) -> (TxEnvelope, Bytes, Addres` |
| `crates/ingress/tests/common/mod.rs:39` | `pub fn sign_legacy(signer: &PrivateKeySigner, nonce: u64) -> Bytes {` |
| `crates/ingress/tests/common/mod.rs:44` | `pub fn sign_legacy_with_gas(signer: &PrivateKeySigner, nonce: u64, gas_limit: u64) -> Byte` |
| `crates/ingress/tests/common/mod.rs:66` | `pub fn sign_eip4844(signer: &PrivateKeySigner, nonce: u64) -> Bytes {` |
| `crates/ingress/tests/sig_verify_batch.rs:18` | `pub fn signed(nonce: u64) -> (AlloyEnvelope, Bytes, Address) {` |
| `crates/log/src/aeron_live/handles/mod.rs:7` | `pub mod simple;` |
| `crates/log/src/aeron_live/handles/mod.rs:8` | `pub mod tx_data;` |
| `crates/log/src/aeron_live/handles/mod.rs:9` | `pub mod tx_receipts;` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:15` | `pub struct LiveTxDataSub {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:20` | `pub fn new(handle: TxDataSubscriberHandle) -> Self {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:33` | `pub struct LiveEpochSub {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:38` | `pub fn new(handle: TxDepositsSubscriberHandle) -> Self {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:49` | `pub struct LiveRemoteEpochSub {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:54` | `pub fn new(handle: TxRemoteEpochsSubscriberHandle) -> Self {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:71` | `pub struct LiveTxErrorPub {` |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:76` | `pub fn new(handle: TxErrorsPublisherHandle) -> Self {` |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:166` | `pub fn spawn_receipt_floor_feed(` |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:244` | `pub type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;` |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:258` | `pub fn spawn_publish_loops<P>(` |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:47` | `pub fn spawn_egress_watermark_feed(` |
| `crates/state/tests/common/mod.rs:16` | `pub fn open_tmp_writer() -> (tempfile::TempDir, WriterHandle) {` |
| `crates/state/tests/common/mod.rs:26` | `pub fn bpos(block: u64) -> BPosition {` |
| `crates/state/tests/common/mod.rs:39` | `pub fn simple_delta(` |
| `crates/state/tests/common/mod.rs:90` | `pub fn slot_key(idx: u64) -> B256 {` |
| `crates/validator/src/bin/kardamom-validator/adoption.rs:113` | `pub fn resync_after_engine_error(` |
| `crates/validator/src/bin/kardamom-validator/adoption.rs:30` | `pub fn adopt_checkpoint_if_fresh(` |
| `crates/validator/src/bin/kardamom-validator/adoption.rs:83` | `pub fn bootstrap_trie_if_adopted(state_dir: &Path, env: &StateEnv) -> Result<()> {` |
| `crates/validator/src/bin/kardamom-validator/args.rs:14` | `pub struct ValidatorFileConfig {` |
| `crates/validator/src/bin/kardamom-validator/args.rs:180` | `pub fn resolve_attester_key(key: &str) -> Result<String> {` |
| `crates/validator/src/bin/kardamom-validator/args.rs:26` | `pub struct Args {` |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:144` | `pub fn spawn_receipts_pump(` |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:174` | `pub fn spawn_commit_poller(` |
| `crates/validator/src/bin/kardamom-validator/pumps.rs:32` | `pub fn spawn_bal_pump(` |

### R11 functions with more than 7 arguments (hidden by allow attributes)

| file:line | args | fn |
|---|---|---|
| crates/bench/src/bin/stm-p2.rs:825 | 16 | run_mdbx_ab |
| crates/stm/src/execute.rs:3295 | 15 | block_tail |
| crates/bench/src/bin/stm-p2.rs:362 | 15 | run_pipelined |
| crates/stm/src/execute.rs:4421 | 12 | execute_one |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:258 | 12 | spawn_publish_loops |
| crates/engine/src/actor/exec_thread.rs:199 | 12 | new |
| crates/engine/src/actor/exec_thread.rs:897 | 12 | spawn_exec |
| crates/exec-core/src/executor/xchain.rs:53 | 11 | execute_xchain_tx |
| crates/bench/src/load/engine.rs:201 | 11 | pacer |
| crates/deployer/src/main.rs:251 | 10 | run_deploy |
| crates/exec-core/src/executor/deposit.rs:43 | 10 | execute_deposit_tx |
| crates/exec-core/src/executor/scope.rs:585 | 10 | execute_once |
| crates/types/src/xchain.rs:268 | 9 | msg_leaf |
| crates/exec-core/src/executor/scope.rs:419 | 9 | execute_tx_decoded |
| crates/exec-core/src/executor/scope.rs:658 | 9 | skip_receipt |
| crates/exec-core/src/executor/scope.rs:711 | 9 | skip |
| crates/exec-core/src/executor/scope.rs:263 | 8 | execute_xchain |
| crates/exec-core/src/executor/scope.rs:348 | 8 | execute_tx |
| crates/state/src/trie/walker.rs:92 | 8 | walk_account |
| crates/state/src/trie/walker.rs:186 | 8 | walk_storage |
| crates/batcher/src/prover_submit.rs:24 | 8 | ? |
| crates/validator/src/parallel/engine.rs:253 | 8 | execute_block_parallel |
| crates/bench/src/load/defi.rs:128 | 8 | sign |
| crates/bench/src/load/engine.rs:104 | 8 | submit_task |
| crates/bench/src/load/mod.rs:366 | 8 | ramp_to_max |
| crates/bench/src/stm/uniswap.rs:115 | 8 | generate |

### R11 pedantic warnings per crate

| crate | total | prod | doc_markdown | missing_errors_doc | must_use_candidate | cast_possible_truncation | cast_precision_loss | missing_panics_doc | map_unwrap_or | default_trait_access | too_many_lines | explicit_iter_loop | other |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| batcher | 182 | 128 | 60 | 39 | 10 | 23 | 3 | 2 | 4 | 0 | 8 | 0 | 33 |
| bench | 215 | 182 | 37 | 17 | 3 | 16 | 80 | 1 | 8 | 3 | 9 | 3 | 38 |
| cluster-adapter | 56 | 54 | 6 | 11 | 19 | 4 | 0 | 6 | 1 | 1 | 0 | 0 | 8 |
| cluster-client | 33 | 33 | 10 | 4 | 15 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 3 |
| da_watcher | 51 | 51 | 3 | 4 | 10 | 6 | 3 | 10 | 0 | 0 | 1 | 0 | 14 |
| deployer | 49 | 41 | 11 | 4 | 20 | 1 | 0 | 0 | 1 | 0 | 0 | 0 | 12 |
| e2e | 232 | 216 | 21 | 104 | 23 | 24 | 5 | 4 | 7 | 3 | 9 | 1 | 31 |
| engine | 173 | 158 | 79 | 20 | 15 | 9 | 5 | 4 | 4 | 2 | 2 | 3 | 30 |
| exec-core | 144 | 116 | 26 | 25 | 23 | 8 | 3 | 3 | 3 | 10 | 2 | 11 | 30 |
| executor | 84 | 27 | 34 | 0 | 1 | 17 | 2 | 1 | 1 | 3 | 4 | 1 | 20 |
| footprint | 45 | 45 | 0 | 0 | 10 | 13 | 14 | 1 | 1 | 0 | 0 | 0 | 6 |
| ingress | 150 | 111 | 57 | 13 | 17 | 9 | 13 | 6 | 3 | 9 | 0 | 2 | 21 |
| interop-feed | 3 | 3 | 0 | 1 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| log | 307 | 296 | 113 | 83 | 43 | 7 | 0 | 8 | 5 | 0 | 1 | 2 | 45 |
| obs | 4 | 1 | 2 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 |
| reconstruct | 8 | 4 | 0 | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 4 |
| sequencer | 216 | 147 | 78 | 14 | 23 | 35 | 8 | 7 | 2 | 16 | 2 | 1 | 30 |
| state | 133 | 118 | 10 | 49 | 34 | 6 | 0 | 4 | 7 | 0 | 2 | 0 | 21 |
| stm | 186 | 171 | 15 | 20 | 4 | 48 | 3 | 17 | 3 | 0 | 10 | 25 | 41 |
| types | 104 | 104 | 32 | 8 | 54 | 4 | 0 | 1 | 2 | 0 | 0 | 0 | 3 |
| validator | 151 | 125 | 36 | 18 | 25 | 13 | 3 | 11 | 5 | 10 | 4 | 4 | 22 |
| **all** | 2526 | 2131 | 630 | 436 | 352 | 244 | 142 | 86 | 57 | 57 | 56 | 53 | 413 |

### R11 pedantic warnings per lint

| lint | count |
|---|---|
| doc_markdown | 630 |
| missing_errors_doc | 436 |
| must_use_candidate | 352 |
| cast_possible_truncation | 244 |
| cast_precision_loss | 142 |
| missing_panics_doc | 86 |
| map_unwrap_or | 57 |
| default_trait_access | 57 |
| too_many_lines | 56 |
| explicit_iter_loop | 53 |
| cast_lossless | 40 |
| needless_pass_by_value | 36 |
| semicolon_if_nothing_returned | 28 |
| items_after_statements | 27 |
| cast_possible_wrap | 25 |
| similar_names | 24 |
| single_match_else | 23 |
| match_same_arms | 20 |
| manual_let_else | 20 |
| unreadable_literal | 20 |
| cast_sign_loss | 18 |
| redundant_closure_for_method_calls | 17 |
| ignored_unit_patterns | 12 |
| return_self_not_must_use | 11 |
| wildcard_imports | 8 |
| struct_excessive_bools | 7 |
| unnecessary_wraps | 6 |
| uninlined_format_args | 6 |
| float_cmp | 6 |
| unnecessary_debug_formatting | 5 |
| needless_continue | 4 |
| elidable_lifetime_names | 4 |
| used_underscore_binding | 3 |
| unused_async | 3 |
| trivially_copy_pass_by_ref | 3 |
| implicit_hasher | 3 |
| unnested_or_patterns | 3 |
| nonminimal_bool | 3 |
| many_single_char_names | 3 |
| match_wildcard_for_single_variants | 3 |
| case_sensitive_file_extension_comparisons | 2 |
| inconsistent_struct_constructor | 2 |
| ptr_as_ptr | 2 |
| large_stack_arrays | 2 |
| fn_params_excessive_bools | 2 |
| format_push_string | 1 |
| unused_self | 1 |
| ref_as_ptr | 1 |
| unused_async_trait_impl | 1 |
| format_collect | 1 |
| needless_raw_string_hashes | 1 |
| duration_suboptimal_units | 1 |
| cloned_instead_of_copied | 1 |
| single_char_pattern | 1 |
| manual_assert | 1 |
| borrow_as_ptr | 1 |
| bool_to_int_with_if | 1 |

### R11 files with the most pedantic warnings

| file | warnings |
|---|---|
| crates/stm/src/execute.rs | 136 |
| crates/bench/src/bin/stm-p2.rs | 94 |
| crates/log/src/testing.rs | 57 |
| crates/engine/src/reader.rs | 51 |
| crates/log/src/config/mod.rs | 39 |
| crates/log/src/recorder.rs | 31 |
| crates/log/src/publisher.rs | 25 |
| crates/engine/src/bin_support.rs | 25 |
| crates/e2e/src/harness/mod.rs | 25 |
| crates/cluster-client/src/protocol.rs | 22 |
| crates/batcher/src/multi_archive_reader.rs | 22 |
| crates/log/src/aeron_live/handles/tx_receipts.rs | 21 |
| crates/footprint/src/oracle.rs | 20 |
| crates/batcher/src/live.rs | 20 |
| crates/sequencer/src/sequencer.rs | 20 |
| crates/log/src/aeron_live/runtime.rs | 19 |
| crates/state/src/schema.rs | 19 |
| crates/log/src/refetch.rs | 18 |
| crates/log/src/aeron_live/handles/simple.rs | 18 |
| crates/validator/src/attester.rs | 18 |
