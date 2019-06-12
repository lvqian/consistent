// Copyright (C) 2012 Numerotron Inc.
// Use of this source code is governed by an MIT-style license
// that can be found in the LICENSE file.

// Package consistent provides a consistent hashing function.
//
// Consistent hashing is often used to distribute requests to a changing set of servers.  For example,
// say you have some cache servers cacheA, cacheB, and cacheC.  You want to decide which cache server
// to use to look up information on a user.
//
// You could use a typical hash table and hash the user id
// to one of cacheA, cacheB, or cacheC.  But with a typical hash table, if you add or remove a server,
// almost all keys will get remapped to different results, which basically could bring your service
// to a grinding halt while the caches get rebuilt.
//
// With a consistent hash, adding or removing a server drastically reduces the number of keys that
// get remapped.
//
// Read more about consistent hashing on wikipedia:  http://en.wikipedia.org/wiki/Consistent_hashing
//
package consistent

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"

	"code.devops.xiaohongshu.com/infra/mikuCluster/proxy/lineProtocol"
)

type uints []uint32

// Len returns the length of the uints array.
func (x uints) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x uints) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x uints) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// ErrEmptyCircle is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyCircle = errors.New("empty circle")

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	circle           map[uint32]lineProtocol.WriteCloser
	members          map[lineProtocol.WriteCloser]bool
	sortedHashes     uints
	NumberOfReplicas int
	count            int64
	scratch          [64]byte
	sync.RWMutex
}

// New creates a new Consistent object with a default setting of 20 replicas for each entry.
//
// To change the number of replicas, set NumberOfReplicas before adding entries.
func New() *Consistent {
	c := new(Consistent)
	c.NumberOfReplicas = 20
	c.circle = make(map[uint32]lineProtocol.WriteCloser)
	c.members = make(map[lineProtocol.WriteCloser]bool)
	return c
}

// elementKey generates a string key for an element with an index.
func (c *Consistent) elementKey(element lineProtocol.WriteCloser, index int) string {
	return strconv.Itoa(index) + element.Name()
}

// Add inserts a string element in the consistent hash.
func (c *Consistent) Add(element lineProtocol.WriteCloser) {
	c.Lock()
	defer c.Unlock()
	c.add(element)
}

// need c.Lock() before calling
func (c *Consistent) add(element lineProtocol.WriteCloser) {
	for i := 0; i < c.NumberOfReplicas; i++ {
		c.circle[c.hashKey(c.elementKey(element, i))] = element
	}
	c.members[element] = true
	c.updateSortedHashes()
	c.count++
}

// Remove removes an element from the hash.
func (c *Consistent) Remove(element lineProtocol.WriteCloser) {
	c.Lock()
	defer c.Unlock()
	c.remove(element)
}

// need c.Lock() before calling
func (c *Consistent) remove(element lineProtocol.WriteCloser) {
	for i := 0; i < c.NumberOfReplicas; i++ {
		delete(c.circle, c.hashKey(c.elementKey(element, i)))
	}
	delete(c.members, element)
	c.updateSortedHashes()
	c.count--
}

// Set sets all the elements in the hash.  If there are existing elements not
// present in elements, they will be removed.
func (c *Consistent) Set(elements []lineProtocol.WriteCloser) {
	c.Lock()
	defer c.Unlock()
	for k := range c.members {
		found := false
		for _, v := range elements {
			if k == v {
				found = true
				break
			}
		}
		if !found {
			c.remove(k)
		}
	}
	for _, v := range elements {
		_, exists := c.members[v]
		if exists {
			continue
		}
		c.add(v)
	}
}

func (c *Consistent) Members() []lineProtocol.WriteCloser {
	c.RLock()
	defer c.RUnlock()
	var m []lineProtocol.WriteCloser
	for k := range c.members {
		m = append(m, k)
	}
	return m
}

// Get returns an element close to where name hashes to in the circle.
func (c *Consistent) Get(name string) (lineProtocol.WriteCloser, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return nil, ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	return c.circle[c.sortedHashes[i]], nil
}

func (c *Consistent) search(key uint32) (i int) {
	f := func(x int) bool {
		return c.sortedHashes[x] > key
	}
	i = sort.Search(len(c.sortedHashes), f)
	if i >= len(c.sortedHashes) {
		i = 0
	}
	return
}

// GetTwo returns the two closest distinct elements to the name input in the circle.
func (c *Consistent) GetTwo(name string) (lineProtocol.WriteCloser, lineProtocol.WriteCloser, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return nil, nil, ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	a := c.circle[c.sortedHashes[i]]

	if c.count == 1 {
		return a, nil, nil
	}

	start := i
	var b lineProtocol.WriteCloser
	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		b = c.circle[c.sortedHashes[i]]
		if b != a {
			break
		}
	}
	return a, b, nil
}

// GetN returns the N closest distinct elements to the name input in the circle.
func (c *Consistent) GetN(name string, n int) ([]lineProtocol.WriteCloser, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.circle) == 0 {
		return nil, ErrEmptyCircle
	}

	if c.count < int64(n) {
		n = int(c.count)
	}

	var (
		key   = c.hashKey(name)
		i     = c.search(key)
		start = i
		res   = make([]lineProtocol.WriteCloser, 0, n)
		elem  = c.circle[c.sortedHashes[i]]
	)

	res = append(res, elem)

	if len(res) == n {
		return res, nil
	}

	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		elem = c.circle[c.sortedHashes[i]]
		if !sliceContainsMember(res, elem) {
			res = append(res, elem)
		}
		if len(res) == n {
			break
		}
	}

	return res, nil
}

func (c *Consistent) hashKey(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	//reallocate if we're holding on to too much (1/4th)
	if cap(c.sortedHashes)/(c.NumberOfReplicas*4) > len(c.circle) {
		hashes = nil
	}
	for k := range c.circle {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	c.sortedHashes = hashes
}

func sliceContainsMember(set []lineProtocol.WriteCloser, member lineProtocol.WriteCloser) bool {
	for _, m := range set {
		if m == member {
			return true
		}
	}
	return false
}
