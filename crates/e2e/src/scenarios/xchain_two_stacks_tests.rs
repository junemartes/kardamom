use super::*;
use alloy_sol_types::{SolCall, sol};

sol!(
    #[allow(
        clippy::too_many_arguments,
        reason = "generated from the contract ABI: `MessageSent` carries 10 fields, fixed by \
                  contracts/src/L2/Outbox.sol"
    )]
    IOutbox,
    concat!(env!("CARGO_WORKSPACE_DIR"), "/contracts/abi/Outbox.json")
);

use IOutbox::sendMessageCall;
use XChain::Callback as SolCb;

/// The hand-rolled `sendMessage` calldata must match `alloy-sol-types`
/// byte for byte. Empty, sub-word, and word+1 payloads exercise the
/// zero-length tail, the right padding, and a two-word tail.
#[test]
fn send_message_calldata_is_byte_identical_to_sol_types() {
    assert_eq!(sendMessageCall::SELECTOR, Outbox::send_message_selector());
    let cb = Callback {
        target: Address::repeat_byte(0x0C),
        gas_limit: 90_000,
        context: B256::repeat_byte(0x1D),
    };
    let target = Address::repeat_byte(0xB9);
    let datas: [&[u8]; 3] = [&b""[..], &[0x01][..], &[0xEE; 33][..]];
    let callbacks = [None, Some(cb)];
    datas
        .iter()
        .flat_map(|&data| callbacks.iter().map(move |&callback| (data, callback)))
        .for_each(|(data, callback)| {
            let c = callback.unwrap_or_default();
            let expect = sendMessageCall {
                destChainId: CHAIN_B_ID,
                target,
                gasLimit: 250_000,
                hops: USER_HOPS,
                data: alloy_primitives::Bytes::copy_from_slice(data),
                cb: SolCb {
                    target: c.target,
                    gasLimit: c.gas_limit,
                    context: c.context,
                },
            }
            .abi_encode();
            assert_eq!(
                send_message_calldata(CHAIN_B_ID, target, 250_000, data, callback),
                expect,
                "data len {}, callback {}",
                data.len(),
                callback.is_some()
            );
        });
}
