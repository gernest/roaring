// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package roaring

import "slices"

type mapContainers struct {
	data map[uint64]*Container
}

func newMapContainers() *mapContainers {
	return &mapContainers{
		data: make(map[uint64]*Container),
	}
}

func NewMapBitmap(a ...uint64) *Bitmap {
	b := &Bitmap{
		Containers: newMapContainers(),
	}
	// We have no way to report this.
	// Because we just created Bitmap, its OpWriter is nil, so there
	// is no code path which would cause Add() to return an error.
	// Therefore, it's safe to swallow this error.
	_, _ = b.Add(a...)
	return b
}

func (btc *mapContainers) Get(key uint64) *Container {

	return btc.data[key]
}

func (btc *mapContainers) Put(key uint64, c *Container) {
	btc.data[key] = c
}

func (btc *mapContainers) Remove(key uint64) {
	delete(btc.data, key)
}

func (btc *mapContainers) GetOrCreate(key uint64) *Container {
	c, ok := btc.data[key]
	if !ok {
		c = NewContainer()
		btc.data[key] = c
	}
	return c
}

func (btc *mapContainers) Count() (n uint64) {
	for _, c := range btc.data {
		n += uint64(c.N())
	}
	return n
}

func (btc *mapContainers) Clone() Containers {
	nbtc := newMapContainers()
	for k, v := range btc.data {
		nbtc.data[k] = v.Clone()
	}
	return nbtc
}

func (btc *mapContainers) Freeze() Containers {
	nbtc := newMapContainers()
	for k, v := range btc.data {
		nbtc.data[k] = v.Freeze()
	}
	return nbtc
}

func (btc *mapContainers) Last() (key uint64, c *Container) {
	for a, b := range btc.data {
		key = a
		c = b
		break
	}
	for a, b := range btc.data {
		if a > key {
			key = a
			c = b
		}
	}
	return
}

func (btc *mapContainers) Size() int {
	return len(btc.data)
}

func (btc *mapContainers) Reset() {
	clear(btc.data)
}

func (btc *mapContainers) ResetN(n int) {
	// we ignore n because it's impractical to preallocate the tree
	btc.Reset()
}

func (btc *mapContainers) Iterator(key uint64) (citer ContainerIterator, found bool) {

	ls := make([]uint64, 0, len(btc.data))
	for k := range btc.data {
		ls = append(ls, k)
	}
	slices.Sort(ls)
	pos := slices.Index(ls, key)
	if pos != -1 {
		pos--
	}
	return &mapIterator{
		ls:  ls,
		e:   btc,
		key: pos,
	}, found
}

func (btc *mapContainers) Repair() {
	for _, c := range btc.data {
		c.Repair()
	}
}

// Update calls fn (existing-container, existed), and expects
// (new-container, write). If write is true, the container is used to
// replace the given container.
func (btc *mapContainers) Update(key uint64, fn func(*Container, bool) (*Container, bool)) {
	c, ok := btc.data[key]
	c, ok = fn(c, ok)
	if ok {
		btc.data[key] = c
	}
}

// UpdateEvery calls fn (existing-container, existed), and expects
// (new-container, write). If write is true, the container is used to
// replace the given container.
func (btc *mapContainers) UpdateEvery(fn func(uint64, *Container, bool) (*Container, bool)) {
	for k, c := range btc.data {
		o, ok := fn(k, c, true)
		if ok {
			btc.data[k] = o
		}
	}
}

type mapIterator struct {
	ls  []uint64
	e   *mapContainers
	key int
}

func (i *mapIterator) Close() {}

func (i *mapIterator) Next() bool {
	i.key++
	return i.key < len(i.ls)
}

func (i *mapIterator) Value() (uint64, *Container) {
	return i.ls[i.key], i.e.data[i.ls[i.key]]
}
