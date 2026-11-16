//! The rule of the queue, with a test frame and an injected offer. The
//! end-to-end proof with a media driver (a refused publish does not delay
//! a live subscription) is `tests/offer_starvation.rs`.

use std::time::Duration;

use super::*;

/// What happened to a [`TestFrame`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Fate {
    Waiting,
    Delivered,
    Failed(String),
}

/// A frame tagged with `marker`, so a test can tell which frame an offer
/// sees.
#[derive(Debug)]
struct TestFrame {
    publication: u32,
    marker: u8,
    deadline: Instant,
    fate: std::rc::Rc<std::cell::RefCell<Fate>>,
}

impl Parked for TestFrame {
    fn publication(&self) -> u32 {
        self.publication
    }

    fn deadline(&self) -> Instant {
        self.deadline
    }

    fn delivered(&mut self, _position: Result<BPosition, LogError>) {
        *self.fate.borrow_mut() = Fate::Delivered;
    }

    fn failed(&mut self, reason: String) {
        *self.fate.borrow_mut() = Fate::Failed(reason);
    }
}

/// The frame `marker` of `publication`, due `due_ms` after `now`, and the
/// view of its fate.
fn frame(
    publication: u32,
    marker: u8,
    now: Instant,
    due_ms: u64,
) -> (TestFrame, std::rc::Rc<std::cell::RefCell<Fate>>) {
    let fate = std::rc::Rc::new(std::cell::RefCell::new(Fate::Waiting));
    (
        TestFrame {
            publication,
            marker,
            deadline: now + Duration::from_millis(due_ms),
            fate: fate.clone(),
        },
        fate,
    )
}

fn delivered() -> Offered {
    Offered::Delivered(Ok(BPosition {
        term_id: 0,
        term_offset: 0,
    }))
}

fn markers(queue: &ParkedQueue<TestFrame>) -> Vec<u8> {
    queue.iter().map(|f| f.marker).collect()
}

#[test]
fn a_delivered_frame_leaves_the_queue() {
    let now = Instant::now();
    let (a, fate) = frame(0, 0xA1, now, 5_000);
    let mut queue = ParkedQueue::from_iter([a]);
    assert_eq!(queue.offer_each(now, |_| delivered()), 0);
    assert!(queue.is_empty());
    assert_eq!(*fate.borrow(), Fate::Delivered);
}

#[test]
fn a_refused_frame_stays_for_the_next_pass() {
    let now = Instant::now();
    let (a, fate) = frame(0, 0xA1, now, 5_000);
    let mut queue = ParkedQueue::from_iter([a]);
    assert_eq!(queue.offer_each(now, |_| Offered::Refused(-2)), 1);
    assert_eq!(queue.len(), 1);
    assert_eq!(*fate.borrow(), Fate::Waiting);
    queue.offer_each(now, |_| delivered());
    assert!(queue.is_empty());
    assert_eq!(*fate.borrow(), Fate::Delivered);
}

/// Two frames of publication 0 and one of publication 1 are refused. Only
/// the head of each publication gets an offer, so the pass counts two.
#[test]
fn a_pass_counts_one_refused_offer_for_each_blocked_publication() {
    let now = Instant::now();
    let (a, _) = frame(0, 0xA1, now, 5_000);
    let (b, _) = frame(0, 0xB2, now, 5_000);
    let (c, _) = frame(1, 0xC3, now, 5_000);
    let mut queue = ParkedQueue::from_iter([a, b, c]);
    assert_eq!(queue.offer_each(now, |_| Offered::Refused(-2)), 2);
    assert_eq!(queue.len(), 3);
    assert_eq!(queue.offer_each(now, |_| delivered()), 0);
    assert!(queue.is_empty());
}

/// The head of publication 0 is refused. The frame behind it gets no
/// offer in that pass, so the stream does not reorder.
#[test]
fn a_frame_never_passes_the_refused_frame_of_its_publication() {
    let now = Instant::now();
    let (a, _) = frame(0, 0xA1, now, 5_000);
    let (b, _) = frame(0, 0xB2, now, 5_000);
    let mut queue = ParkedQueue::from_iter([a, b]);
    let mut offered = Vec::new();
    queue.offer_each(now, |f| {
        offered.push(f.marker);
        Offered::Refused(-2)
    });
    assert_eq!(offered, [0xA1]);
    assert_eq!(markers(&queue), [0xA1, 0xB2]);
}

#[test]
fn a_refused_publication_does_not_block_another_one() {
    let now = Instant::now();
    let (a, fate_a) = frame(0, 0xA1, now, 5_000);
    let (b, fate_b) = frame(1, 0xB2, now, 5_000);
    let mut queue = ParkedQueue::from_iter([a, b]);
    queue.offer_each(now, |f| {
        if f.publication == 0 {
            Offered::Refused(-2)
        } else {
            delivered()
        }
    });
    assert_eq!(markers(&queue), [0xA1]);
    assert_eq!(*fate_a.borrow(), Fate::Waiting);
    assert_eq!(*fate_b.borrow(), Fate::Delivered);
}

#[test]
fn a_refused_frame_fails_at_its_deadline_with_the_status() {
    let now = Instant::now();
    let (a, fate) = frame(0, 0xA1, now, 0);
    let mut queue = ParkedQueue::from_iter([a]);
    queue.offer_each(now, |_| Offered::Refused(-1));
    assert!(queue.is_empty());
    let Fate::Failed(reason) = fate.borrow().clone() else {
        panic!("the frame did not fail: {:?}", fate.borrow());
    };
    assert!(reason.contains("NOT_CONNECTED"), "{reason}");
}

/// The frame behind a refused head fails at its own deadline, with no
/// offer, and the head stays.
#[test]
fn a_frame_behind_a_refused_head_fails_at_its_own_deadline() {
    let now = Instant::now();
    let (head, head_fate) = frame(0, 0xA1, now, 5_000);
    let (tail, tail_fate) = frame(0, 0xB2, now, 0);
    let mut queue = ParkedQueue::from_iter([head, tail]);
    let mut offered = Vec::new();
    queue.offer_each(now, |f| {
        offered.push(f.marker);
        Offered::Refused(-2)
    });
    assert_eq!(offered, [0xA1]);
    assert_eq!(markers(&queue), [0xA1]);
    assert_eq!(*head_fate.borrow(), Fate::Waiting);
    let Fate::Failed(reason) = tail_fate.borrow().clone() else {
        panic!("the tail did not fail: {:?}", tail_fate.borrow());
    };
    assert!(reason.contains("expired while queued"), "{reason}");
}

#[test]
fn a_frame_of_an_unknown_publication_fails_at_once() {
    let now = Instant::now();
    let (a, fate) = frame(9, 0xA1, now, 5_000);
    let mut queue = ParkedQueue::from_iter([a]);
    queue.offer_each(now, |_| Offered::UnknownPublication);
    assert!(queue.is_empty());
    let Fate::Failed(reason) = fate.borrow().clone() else {
        panic!("the frame did not fail: {:?}", fate.borrow());
    };
    assert!(reason.contains("unknown pub_id 9"), "{reason}");
}
