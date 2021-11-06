use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::ops::Fn;

use crate::util::*;

pub struct Locking {
    id: Tid,
    want: Vec<Oid>,
    got: Vec<Oid>,
    locked: Box<dyn Fn(Tid)>,
}
    
pub struct LockManager {
    locks: HashSet<Oid>,
    waiting: HashMap<Oid, VecDeque<Tid>>,
    locking: HashMap<Tid, Locking>,
}

impl LockManager {

    pub fn new() -> LockManager {
        LockManager {
            locks: HashSet::new(),
            waiting: HashMap::new(),
            locking: HashMap::new(),
        }
    }

    pub fn lock(&mut self, id: Tid, want: Vec<Oid>, locked: Box<dyn Fn(Tid)>) {
        self.lock_waiting(
            Locking { id: id, want: want, got: vec![], locked: locked });
    }

    fn lock_waiting(&mut self, mut locking: Locking) {
        let id = locking.id;
        { // Limit lifetime of locker borrow below :(
            let want = &mut locking.want;
            let got =  &mut locking.got;
            while ! want.is_empty() {
                let oid = want.last().unwrap().clone();
                if self.locks.contains(&oid) {
                    if self.waiting.contains_key(&oid) {
                        self.waiting.get_mut(&oid).unwrap().push_back(id);
                    }
                    else {
                        let mut waiting: VecDeque<Tid> = VecDeque::new();
                        waiting.push_back(id);
                        self.waiting.insert(oid, waiting);
                    }
                    break;
                }
                else {
                    self.locks.insert(oid);
                    got.push(want.pop().unwrap());
                }
            }
            if want.is_empty() {
                (*locking.locked)(locking.id)
            }
        }
        self.locking.insert(id, locking);

    }

    pub fn release(&mut self, id: &Tid) {
        // Release any locks held for the given id. This has no effect of no
        // locks are held.
        if let Some(mut locking) = self.locking.remove(id) {
            while ! locking.got.is_empty() {
                let oid = locking.got.pop().unwrap();
                self.locks.remove(&oid);
                if self.waiting.contains_key(&oid) {
                    let tid_waiting =
                        self.waiting.get_mut(&oid).unwrap().pop_front();
                    if self.waiting.get(&oid).unwrap().is_empty() {
                        self.waiting.remove(&oid);
                    }
                    if let Some(tid) = tid_waiting {
                        if let Some(locking) = self.locking.remove(&tid) {
                            self.lock_waiting(locking);
                        }
                    }
                }
            }
        }
    }
}


// ======================================================================

#[cfg(test)]
mod tests {

    use super::*;

    struct TestLocker { id: Tid, pub is_locked: bool }
    impl TestLocker {
        fn locked(&mut self) { self.is_locked = true; }
    }
    fn newt(id: u64) -> Ob<TestLocker> {
        new_ob(TestLocker {id: p64(id), is_locked: false})
    }
    fn oids(v: Vec<u64>) -> Vec<Oid> {
        v.iter().map(| i | p64(*i)).collect::<Vec<Tid>>()
    }
    fn lock(lm: &mut LockManager, locker: Ob<TestLocker>, oids: Vec<u64>) {
        let id = locker.borrow().id;
        let orig_id = id.clone();
        lm.lock(id,
                oids.iter().map(| i | p64(*i)).collect::<Vec<Oid>>(),
                Box::new(move | lid | {
                    assert_eq!(lid, orig_id);
                    locker.borrow_mut().locked()
                }),
        )
    }
    
    #[test]
    fn works() {
        let mut lm = LockManager::new();
        
        let l1_123 = newt(1);
        lock(&mut lm, l1_123.clone(), vec![1, 2, 3]);
        assert!(l1_123.borrow().is_locked);

        let l2_12 = newt(2);
        let l3_12 = newt(3);
        let l4_3 = newt(4);
        lock(&mut lm, l2_12.clone(), vec![1, 2]);
        lock(&mut lm, l3_12.clone(), vec![1, 2]);
        lock(&mut lm, l4_3.clone(), vec![3]);
        assert!(  l1_123.borrow().is_locked);
        assert!(! l2_12.borrow().is_locked);
        assert!(! l3_12.borrow().is_locked);
        assert!(! l4_3.borrow().is_locked);

        let l5_4 = newt(5);
        lock(&mut lm, l5_4.clone(), vec![4]);
        assert!(  l1_123.borrow().is_locked);
        assert!(! l2_12.borrow().is_locked);
        assert!(! l3_12.borrow().is_locked);
        assert!(! l4_3.borrow().is_locked);
        assert!(  l5_4.borrow().is_locked);

        lm.release(&p64(1));
        assert!(  l2_12.borrow().is_locked);
        assert!(! l3_12.borrow().is_locked);
        assert!(  l4_3.borrow().is_locked);
        assert!(  l5_4.borrow().is_locked);

        lm.release(&p64(2));
        assert!(  l3_12.borrow().is_locked);
        assert!(  l4_3.borrow().is_locked);
        assert!(  l5_4.borrow().is_locked);
    }
}
