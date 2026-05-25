package topk

type bucket struct {
	count int
	head  *slot
	tail  *slot
	prev  *bucket
	next  *bucket
}

type slot struct {
	query  string
	count  int
	errVal int
	parent *bucket
	prev   *slot
	next   *slot
}

type Entry struct {
	Query string
	Count int
	Error int
}

type SpaceSaving struct {
	capacity  int
	slots     map[string]*slot
	minBucket *bucket
}

func New(capacity int) *SpaceSaving {
	if capacity <= 0 {
		panic("topk: capacity must be positive")
	}
	return &SpaceSaving{
		capacity: capacity,
		slots:    make(map[string]*slot, capacity),
	}
}

func (s *SpaceSaving) Len() int {
	return len(s.slots)
}

func (s *SpaceSaving) Cap() int {
	return s.capacity
}

func (s *SpaceSaving) Add(query string) {
	s.AddN(query, 1)
}

func (s *SpaceSaving) AddN(query string, n int) {
	if n <= 0 {
		return
	}
	if existing, ok := s.slots[query]; ok {
		s.bumpSlot(existing, existing.count+n)
		return
	}

	if len(s.slots) < s.capacity {
		s.insertNewSlot(query, n, 0)
		return
	}

	victim := s.minBucket.head
	victimBucket := s.minBucket
	newCount := victimBucket.count + n
	errVal := victimBucket.count

	delete(s.slots, victim.query)
	s.detachSlot(victim)

	if victimBucket.head == nil {
		s.removeEmptyBucket(victimBucket)
	}

	victim.query = query
	victim.count = newCount
	victim.errVal = errVal
	s.slots[query] = victim
	s.attachSlotToBucketWithCount(victim, newCount)
}

func (s *SpaceSaving) TopK(n int) []Entry {
	if n <= 0 {
		return nil
	}
	if n > len(s.slots) {
		n = len(s.slots)
	}

	result := make([]Entry, 0, n)

	b := s.findMaxBucket()
	for b != nil && len(result) < n {
		for sl := b.head; sl != nil && len(result) < n; sl = sl.next {
			result = append(result, Entry{
				Query: sl.query,
				Count: sl.count,
				Error: sl.errVal,
			})
		}
		b = b.prev
	}

	return result
}

func (s *SpaceSaving) Merge(other *SpaceSaving) {
	if other == nil {
		return
	}
	for b := other.minBucket; b != nil; b = b.next {
		for sl := b.head; sl != nil; sl = sl.next {
			s.AddN(sl.query, sl.count)
		}
	}
}

func (s *SpaceSaving) findMaxBucket() *bucket {
	b := s.minBucket
	if b == nil {
		return nil
	}
	for b.next != nil {
		b = b.next
	}
	return b
}

func (s *SpaceSaving) bumpSlot(sl *slot, newCount int) {
	oldBucket := sl.parent

	s.detachSlot(sl)
	sl.count = newCount
	s.attachSlotToBucketWithCount(sl, newCount)

	if oldBucket.head == nil {
		s.removeEmptyBucket(oldBucket)
	}
}

func (s *SpaceSaving) insertNewSlot(query string, count, errVal int) {
	sl := &slot{
		query:  query,
		count:  count,
		errVal: errVal,
	}
	s.slots[query] = sl
	s.attachSlotToBucketWithCount(sl, count)
}

func (s *SpaceSaving) attachSlotToBucketWithCount(sl *slot, count int) {
	var target *bucket
	var insertAfter *bucket

	b := s.minBucket
	for b != nil && b.count < count {
		insertAfter = b
		b = b.next
	}
	if b != nil && b.count == count {
		target = b
	} else {
		target = s.createBucketAfter(count, insertAfter)
	}

	sl.parent = target
	sl.prev = target.tail
	sl.next = nil
	if target.tail != nil {
		target.tail.next = sl
	} else {
		target.head = sl
	}
	target.tail = sl
}

func (s *SpaceSaving) detachSlot(sl *slot) {
	b := sl.parent
	if sl.prev != nil {
		sl.prev.next = sl.next
	} else {
		b.head = sl.next
	}
	if sl.next != nil {
		sl.next.prev = sl.prev
	} else {
		b.tail = sl.prev
	}
	sl.prev = nil
	sl.next = nil
	sl.parent = nil
}

func (s *SpaceSaving) createBucketAfter(count int, after *bucket) *bucket {
	nb := &bucket{count: count}
	if after == nil {
		nb.next = s.minBucket
		if s.minBucket != nil {
			s.minBucket.prev = nb
		}
		s.minBucket = nb
	} else {
		nb.prev = after
		nb.next = after.next
		if after.next != nil {
			after.next.prev = nb
		}
		after.next = nb
	}
	return nb
}

func (s *SpaceSaving) removeEmptyBucket(b *bucket) {
	if b.prev != nil {
		b.prev.next = b.next
	} else {
		s.minBucket = b.next
	}
	if b.next != nil {
		b.next.prev = b.prev
	}
	b.prev = nil
	b.next = nil
}
