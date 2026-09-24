//! The monotone write rule, as the Lua script Redis runs atomically per
//! account.

/// Apply one account row.
///
/// `KEYS[1]` is the account hash. `ARGV[1]` is the position tag, `ARGV[2]`
/// the nonce, `ARGV[3]` the balance as a decimal string. Tags compare as
/// strings; see `keys`.
///
/// Returns [`APPLIED`] when the row is newer than the stored one,
/// `0` when it is older or equal with the same content, and
/// [`DISAGREED`] when it is equal in position but differs in content. The
/// stored value never changes on an equal position: the first row wins,
/// and the caller counts the disagreement.
pub const APPLY_ROW: &str = r"
local stored = redis.call('HGET', KEYS[1], 'tx_idx')
if stored == false or ARGV[1] > stored then
  redis.call('HSET', KEYS[1], 'nonce', ARGV[2], 'balance', ARGV[3], 'tx_idx', ARGV[1])
  return 1
end
if ARGV[1] == stored then
  local nonce = redis.call('HGET', KEYS[1], 'nonce')
  local balance = redis.call('HGET', KEYS[1], 'balance')
  if nonce ~= ARGV[2] or balance ~= ARGV[3] then
    return -1
  end
end
return 0
";

/// The row was newer and is stored.
pub const APPLIED: i64 = 1;
/// The row was equal in position and different in content.
pub const DISAGREED: i64 = -1;
