use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::ptr::NonNull;
use std::sync::Arc;

/// Removes the entry from the cache, cleaning up any values if necessary.
pub(super) fn remove_entry_pointer(mut node: LfuEntry, freq_list: &mut FrequencyList) {
    if let Some(mut next) = node.next {
        let next = unsafe { next.as_mut() };
        next.prev = node.prev;
    }

    if let Some(mut prev) = node.prev {
        let prev = unsafe { prev.as_mut() };
        prev.next = node.next;
    } else {
        unsafe { node.owner.as_mut() }.elements = node.next;
    }

    let owner = unsafe { node.owner.as_mut() };
    if owner.elements.is_none() {
        if let Some(mut next) = owner.next {
            let next = unsafe { next.as_mut() };
            next.prev = owner.prev;
        }

        if let Some(mut prev) = owner.prev {
            let prev = unsafe { prev.as_mut() };
            prev.next = owner.next;
        } else {
            freq_list.head = owner.next;
        }

        owner.next = None;

        // Drop the node, since the node is empty now.
        unsafe { Box::from_raw(owner) };
        freq_list.len -= 1;
    };
}

#[derive(Default, Eq, Ord, PartialOrd, Debug)]
pub(super) struct Node {
    pub(super) next: Option<NonNull<Self>>,
    pub(super) prev: Option<NonNull<Self>>,
    // youngest entry
    pub(super) elements: Option<NonNull<LfuEntry>>,
    // eldest entry
    pub(super) root: Option<NonNull<LfuEntry>>,
    pub(super) frequency: usize,
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.frequency == other.frequency
    }
}

#[cfg(not(tarpaulin_include))]
impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.frequency);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut ptr) = self.next {
            // SAFETY: self is exclusively accessed
            unsafe { Box::from_raw(ptr.as_mut()) };
        }
    }
}

impl Node {
    pub(super) fn create_increment(&mut self) {
        // Initialize new node with links to current and next's next
        let new_node = Box::new(Self {
            next: self.next,
            prev: Some(self.into()),
            elements: None,
            root: None,
            frequency: self.frequency + 1,
        });

        // Fix links to point to new node
        let node: NonNull<_> = Box::leak(new_node).into();

        // Fix next element's previous reference to new node
        if let Some(mut next_node) = self.next {
            // SAFETY: self is exclusively accessed
            let node_ptr = unsafe { next_node.as_mut() };
            node_ptr.prev = Some(node);
        }
        // Fix current element's next reference to new node
        self.next = Some(node);
    }

    /// Pushes the entry to the front of the list
    pub(super) fn push(&mut self, mut entry: NonNull<LfuEntry>) {
        if let Some(mut head) = self.elements {
            // SAFETY: self is exclusively accessed
            let head_ptr = unsafe { head.as_mut() };
            head_ptr.prev = Some(entry);
        }
        // SAFETY: self is exclusively accessed
        let entry_ptr = unsafe { entry.as_mut() };
        entry_ptr.next = self.elements;

        // Update internals
        entry_ptr.owner = self.into();

        // Fix previous
        entry_ptr.prev = None;
        self.elements = Some(entry);
        // Fix next
        match self.root {
            Some(_) => {}
            None => self.root = Some(entry),
        }
    }

    /// pops the root (the eldest node)
    pub(super) fn pop(&mut self) -> Option<NonNull<LfuEntry>> {
        if let Some(mut node_ptr) = self.root {
            // SAFETY: self is exclusively accessed
            let node = unsafe { node_ptr.as_mut() };

            if let Some(mut prev) = node.prev {
                // SAFETY: self is exclusively accessed
                let prev = unsafe { prev.as_mut() };
                prev.next = None;
            }

            self.root = node.prev;

            node.next = None;
            node.prev = None;

            return Some(node_ptr);
        }

        None
    }

    pub(super) fn len(&self) -> usize {
        let mut count = 0;
        let mut head = self.elements;
        while let Some(cur_node) = head {
            let cur_node = unsafe { cur_node.as_ref() };
            count += 1;
            head = cur_node.next;
        }
        count
    }
}

// #[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(super) struct LfuEntry {
    /// We still need to keep a linked list implementation for O(1)
    /// in-the-middle removal.
    pub(super) next: Option<NonNull<Self>>,
    pub(super) prev: Option<NonNull<Self>>,
    /// Instead of traversing up to the frequency node, we just keep a reference
    /// to the owning node. This ensures that entry movement is an O(1)
    /// operation.
    pub(super) owner: NonNull<Node>,
    /// We need to maintain a pointer to the key as we need to remove the
    /// lookup table entry on lru popping, and we need the key to properly fetch
    /// the correct entry (the hash itself is not guaranteed to return the
    /// correct entry).
    pub(super) key: Arc<Vec<u8>>,
    // pub(super) value: T,
}

impl LfuEntry {
    #[must_use]
    pub(super) fn new(owner: NonNull<Node>, key: Arc<Vec<u8>>) -> Self {
        Self {
            next: None,
            prev: None,
            owner,
            key,
            // value,
        }
    }
}

#[derive(Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(super) struct FrequencyList {
    pub(super) head: Option<NonNull<Node>>,
    pub(super) len: usize,
}

#[cfg(not(tarpaulin_include))]
impl Debug for FrequencyList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut dbg = f.debug_struct("FrequencyList");
        dbg.field("len", &self.len);

        let mut node = self.head;
        while let Some(cur_node) = node {
            let cur_node = unsafe { cur_node.as_ref() };
            dbg.field(
                &format!("node freq {} num elements", &cur_node.frequency),
                &cur_node.len(),
            );
            node = cur_node.next;
        }

        dbg.finish()
    }
}

impl Drop for FrequencyList {
    fn drop(&mut self) {
        if let Some(mut ptr) = self.head {
            // SAFETY: self is exclusively accessed
            unsafe { Box::from_raw(ptr.as_mut()) };
        }
    }
}

impl FrequencyList {
    pub(super) fn new() -> Self {
        Self { head: None, len: 0 }
    }

    /// Inserts an item into the frequency list, returning a pointer to the
    /// item. Callers must make sure to free the returning pointer, usually via
    /// `Box::from_raw(foo.as_ptr())`.
    pub(super) fn insert(&mut self, key: Arc<Vec<u8>>) -> NonNull<LfuEntry> {
        let mut head = match self.head {
            Some(head) if unsafe { head.as_ref() }.frequency == 0 => head,
            _ => self.init_front(),
        };

        let entry = Box::new(LfuEntry::new(head, key));
        let entry = NonNull::from(Box::leak(entry));
        // SAFETY: self is exclusively accessed
        unsafe { head.as_mut() }.push(entry);
        entry
    }

    fn init_front(&mut self) -> NonNull<Node> {
        let node = Box::new(Node {
            next: self.head,
            prev: None,
            elements: None,
            root: None,
            frequency: 0,
        });

        let node = NonNull::from(Box::leak(node));

        if let Some(mut head) = self.head {
            // SAFETY: self is exclusively accessed
            if let Some(mut next) = unsafe { head.as_ref() }.next {
                // SAFETY: self is exclusively accessed
                let next = unsafe { next.as_mut() };
                next.prev = Some(head);
            }

            let head = unsafe { head.as_mut() };
            head.prev = Some(node);
        }

        self.head = Some(node);
        self.len += 1;

        node
    }

    pub(super) fn update(&mut self, mut entry: NonNull<LfuEntry>) {
        let entry = unsafe { entry.as_mut() };
        // Remove the entry from the node.
        // SAFETY: self is exclusively accessed
        if let Some(mut prev) = entry.prev {
            unsafe { prev.as_mut() }.next = entry.next;
        } else {
            unsafe { entry.owner.as_mut() }.elements = entry.next;
        }

        if let Some(mut next) = entry.next {
            unsafe { next.as_mut() }.prev = entry.prev;
        }

        // Generate the next frequency list node if it doesn't exist or isn't
        // n + 1 of the current node's frequency.
        // SAFETY: self is exclusively accessed
        let freq_list_node = unsafe { entry.owner.as_mut() };

        let freq_list_node_freq = freq_list_node.frequency;
        match freq_list_node.next {
            // SAFETY: self is exclusively accessed
            Some(node) if unsafe { node.as_ref() }.frequency == freq_list_node_freq + 1 => (),
            _ => {
                freq_list_node.create_increment();
                self.len += 1;
            }
        }

        // Drop frequency list node if it contains no elements
        if freq_list_node.elements.is_none() {
            if let Some(mut prev) = freq_list_node.prev {
                // SAFETY: self is exclusively accessed
                unsafe { prev.as_mut() }.next = freq_list_node.next;
            } else {
                self.head = freq_list_node.next;
            }

            if let Some(mut next) = freq_list_node.next {
                // SAFETY: self is exclusively accessed
                unsafe { next.as_mut() }.prev = freq_list_node.prev;
            }

            let mut boxed = unsafe { Box::from_raw(freq_list_node) };

            // Insert item into next_owner
            unsafe { boxed.next.unwrap().as_mut() }.push(entry.into());

            // Because our drop implementation of Node recursively frees the
            // the next value, we need to unset the next value before dropping
            // the box lest we free the entire list.
            boxed.next = None;
            self.len -= 1;
        } else {
            // Insert item into next_owner
            unsafe { freq_list_node.next.unwrap().as_mut() }.push(entry.into());
        }
    }

    pub(super) fn pop_lfu(&mut self) -> Option<NonNull<LfuEntry>> {
        self.head
            .as_mut()
            .and_then(|node| unsafe { node.as_mut() }.pop())
    }

    pub(super) fn frequencies(&self) -> Vec<usize> {
        let mut freqs = vec![];
        let mut cur_head = self.head;
        while let Some(node) = cur_head {
            let cur_node = unsafe { node.as_ref() };
            freqs.push(cur_node.frequency);
            cur_head = cur_node.next;
        }

        freqs
    }
}

#[cfg(test)]
mod tests {
    use super::FrequencyList;
    use super::LfuEntry;
    use std::{ptr::NonNull, sync::Arc};

    fn init_list() -> FrequencyList {
        FrequencyList::new()
    }

    #[test]
    fn new_constructs_dangling_entry_with_owner() {
        let owner = NonNull::dangling();
        let key = Arc::new(Vec::from([1u8]));
        let entry = LfuEntry::new(owner, Arc::clone(&key));

        assert!(entry.next.is_none());
        assert!(entry.prev.is_none());
        assert_eq!(entry.owner, owner);
        // assert_eq!(entry.key, key);
    }

    #[test]
    fn new_is_empty() {
        let list = init_list();
        assert!(list.head.is_none());
        assert_eq!(list.len, 0);
        assert!(list.frequencies().is_empty());
    }

    #[test]
    fn insert() {
        let mut list = init_list();
        let entry = unsafe { Box::from_raw(list.insert(Arc::new(Vec::from([1u8]))).as_ptr()) };
        assert_eq!(entry.prev, None);
        assert_eq!(entry.next, None);
        assert_eq!(entry.owner, list.head.unwrap());
    }

    #[test]
    fn insert_non_empty() {
        let mut list = init_list();
        let entry_0 = NonNull::new(list.insert(Arc::new(Vec::from([1u8]))).as_ptr()).unwrap();
        let entry_1 = NonNull::new(list.insert(Arc::new(Vec::from([3u8]))).as_ptr()).unwrap();

        let entry_0_ref = unsafe { entry_0.as_ref() };
        let entry_1_ref = unsafe { entry_1.as_ref() };

        // validate entry_1
        assert_eq!(entry_1_ref.prev, None);
        assert_eq!(entry_1_ref.next, Some(entry_0));
        assert_eq!(entry_1_ref.owner, list.head.unwrap());

        // validate entry_0
        assert_eq!(entry_0_ref.prev, Some(entry_1));
        assert_eq!(entry_0_ref.next, None);
        assert_eq!(entry_0_ref.owner, list.head.unwrap());

        unsafe {
            Box::from_raw(entry_0.as_ptr());
            Box::from_raw(entry_1.as_ptr());
        }
    }

    #[test]
    fn insert_non_empty_non_freq_zero() {
        let mut list = init_list();
        let mut entry_0 =
            unsafe { Box::from_raw(list.insert(Arc::new(Vec::from([1u8]))).as_ptr()) };
        list.update(NonNull::from(&mut *entry_0));
        let entry_1 = unsafe { Box::from_raw(list.insert(Arc::new(Vec::from([3u8]))).as_ptr()) };

        // validate entry_0
        assert_eq!(entry_0.prev, None);
        assert_eq!(entry_0.next, None);
        assert_ne!(entry_0.owner, list.head.unwrap());

        // validate entry_1
        assert_eq!(entry_1.prev, None);
        assert_eq!(entry_1.next, None);
        assert_eq!(entry_1.owner, list.head.unwrap());
    }

    #[test]
    fn init_front_empty() {
        let mut list = init_list();
        let front_node = list.init_front();
        assert_eq!(list.head, Some(front_node));
        assert_eq!(list.len, 1);

        let front_node = unsafe { front_node.as_ref() };
        assert_eq!(front_node.prev, None);
        assert_eq!(front_node.next, None);
    }

    #[test]
    fn init_front_non_empty() {
        let mut list = init_list();

        let back_node = list.init_front();
        assert_eq!(list.head, Some(back_node));
        assert_eq!(list.len, 1);
        {
            let back_node = unsafe { back_node.as_ref() };
            assert_eq!(back_node.prev, None);
            assert_eq!(back_node.next, None);
        }

        let middle_node = list.init_front();
        assert_eq!(list.head, Some(middle_node));
        assert_eq!(list.len, 2);
        {
            // validate middle node connections
            let middle_node = unsafe { middle_node.as_ref() };
            assert_eq!(middle_node.prev, None);
            assert_eq!(middle_node.next, Some(back_node));
        }
        {
            // validate back node connections
            let back_node = unsafe { back_node.as_ref() };
            assert_eq!(back_node.prev, Some(middle_node));
            assert_eq!(back_node.next, None);
        }

        let front_node = list.init_front();
        assert_eq!(list.head, Some(front_node));
        assert_eq!(list.len, 3);
        {
            // validate front node connections
            let front_node = unsafe { front_node.as_ref() };
            assert_eq!(front_node.prev, None);
            assert_eq!(front_node.next, Some(middle_node));
        }

        {
            // validate middle node connections
            let middle_node = unsafe { middle_node.as_ref() };
            assert_eq!(middle_node.prev, Some(front_node));
            assert_eq!(middle_node.next, Some(back_node));
        }
        {
            // validate back node connections
            let back_node = unsafe { back_node.as_ref() };
            assert_eq!(back_node.prev, Some(middle_node));
            assert_eq!(back_node.next, None);
        }
    }

    #[test]
    fn update_removes_empty_node() {
        let mut list = init_list();
        let entry = list.insert(Arc::new(Vec::from([1u8])));

        list.update(entry);
        assert_eq!(unsafe { list.head.unwrap().as_ref() }.frequency, 1);
        list.update(entry);
        assert_eq!(unsafe { list.head.unwrap().as_ref() }.frequency, 2);

        // unleak entry
        unsafe { Box::from_raw(entry.as_ptr()) };
    }

    #[test]
    fn update_does_not_remove_non_empty_node() {
        let mut list = init_list();
        let entry_0 = list.insert(Arc::new(Vec::from([1u8])));
        let entry_1 = list.insert(Arc::new(Vec::from([3u8])));

        list.update(entry_0);
        assert_eq!(unsafe { list.head.unwrap().as_ref() }.frequency, 0);
        assert_eq!(list.frequencies(), vec![0, 1]);
        list.update(entry_1);
        list.update(entry_0);
        assert_eq!(unsafe { list.head.unwrap().as_ref() }.frequency, 1);
        assert_eq!(list.frequencies(), vec![1, 2]);

        // unleak entry
        unsafe { Box::from_raw(entry_0.as_ptr()) };
        unsafe { Box::from_raw(entry_1.as_ptr()) };
    }

    #[test]
    fn update_correctly_removes_in_middle_nodes() {
        let mut list = init_list();
        let entry_0 = list.insert(Arc::new(Vec::from([1u8])));
        let entry_1 = list.insert(Arc::new(Vec::from([3u8])));

        list.update(entry_0);
        assert_eq!(unsafe { list.head.unwrap().as_ref() }.frequency, 0);
        assert_eq!(list.frequencies(), vec![0, 1]);
        list.update(entry_0);
        assert_eq!(unsafe { list.head.unwrap().as_ref() }.frequency, 0);
        assert_eq!(list.frequencies(), vec![0, 2]);

        // unleak entry
        unsafe { Box::from_raw(entry_0.as_ptr()) };
        unsafe { Box::from_raw(entry_1.as_ptr()) };
    }
}
