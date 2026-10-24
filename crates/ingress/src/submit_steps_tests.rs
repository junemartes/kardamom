use kardamom_obs::pipeline::Step;

use super::Timing;
use crate::rendered_metrics_tests::Rendered;

/// A step with an early return: the error path returns before the end.
fn checked(step: &Step, fail: bool) -> Result<(), ()> {
    let _timing = Timing::start(step);
    if fail {
        return Err(());
    }
    Ok(())
}

#[test]
fn a_timing_records_one_sample_on_each_return_path() {
    let text = Rendered::of(|| {
        let step = Step::new("ingress.test");
        assert_eq!(checked(&step, true), Err(()));
        assert_eq!(checked(&step, false), Ok(()));
    });
    assert_eq!(
        text.value(r#"kardamom_step_seconds_count{step="ingress.test"}"#),
        2.0
    );
}
