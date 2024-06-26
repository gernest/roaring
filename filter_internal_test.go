// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package roaring

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/gernest/roaring/shardwidth"
)

// For each container key i from 1 to (shard width in containers), we
// populate Bit i of that container in rows 0, K, etc. up to 100.
var filterSampleData *Bitmap
var filterSamplePositions [][]uint64
var prepareSampleData sync.Once

const sampleDataSize = 100

func requireSampleData(tb testing.TB) {
	prepareSampleData.Do(func() {
		b := NewBitmap(0)
		filterSamplePositions = make([][]uint64, rowWidth)
		for i := 1; i < rowWidth; i++ {
			filterSamplePositions[i] = make([]uint64, 0, sampleDataSize/i)
			base := (uint64(i) << 16) + uint64(i)
			for row := 0; row < sampleDataSize; row += i {
				pos := (uint64(row) << shardwidth.Exponent) + base
				filterSamplePositions[i] = append(filterSamplePositions[i], pos)
				_, _ = b.Add(pos)
			}
		}
		filterSampleData = b
	})
}

func applyFilter(tb testing.TB, data *Bitmap, filter *BitmapBitmapFilter) error {
	iter, _ := data.Containers.Iterator(0)
	return ApplyFilterToIterator(filter, iter)
}

func getRows(tb testing.TB, data *Bitmap, filters ...BitmapFilter) []uint64 {
	var rows []uint64
	callback := func(row uint64) error {
		rows = append(rows, row)
		return nil
	}
	iter, _ := data.Containers.Iterator(0)
	filter := NewBitmapRowFilter(callback, filters...)
	err := ApplyFilterToIterator(filter, iter)
	if err != nil {
		tb.Fatalf("unexpected error: %v", err)
	}
	return rows
}

func compareSlices(tb testing.TB, name string, s1, s2 []uint64) {
	if len(s1) != len(s2) {
		tb.Fatalf("slice length mismatch %q: expected %d items %d, got %d items %d",
			name, len(s1), s1, len(s2), s2)
	}
	for i, v := range s1 {
		if s2[i] != v {
			tb.Fatalf("row mismatch %q: expected item %d to be %d, got %d",
				name, i, s1[i], s2[i])
		}
	}
}

func TestBaseFilter(t *testing.T) {
	requireSampleData(t)
	expected := make([]uint64, sampleDataSize)
	// we expect every row, because stride1 hits them all
	for i := range expected {
		expected[i] = uint64(i)
	}
	rows := getRows(t, filterSampleData)
	compareSlices(t, "base", expected, rows)
}

func TestColumnFilter(t *testing.T) {
	requireSampleData(t)
	expected := make([]uint64, 0, sampleDataSize)
	for i := 1; i < rowWidth; i++ {
		t.Run(fmt.Sprintf("stride%d", i), func(t *testing.T) {
			expected = expected[:0]
			base := (uint64(i) << 16) + uint64(i)
			for row := 0; row < sampleDataSize; row += i {
				expected = append(expected, uint64(row))
			}
			rows := getRows(t, filterSampleData, NewBitmapColumnFilter(base))
			compareSlices(t, "stride", expected, rows)
		})
	}
}

func TestRowsFilter(t *testing.T) {
	requireSampleData(t)
	rowSet := []uint64{0, 1, 2, 3}
	expected := []uint64{0, 2}
	// we expect every row to be present, because every row is present in stride1
	rows := getRows(t, filterSampleData, NewBitmapRowsFilter(rowSet))
	compareSlices(t, "initial", rowSet, rows)
	// Check for stride-2 values that overlap with rowSet
	rows = getRows(t, filterSampleData, NewBitmapRowsFilter(rowSet), NewBitmapColumnFilter((2<<16)+2))
	compareSlices(t, "rows-column", expected, rows)
	rows = getRows(t, filterSampleData, NewBitmapColumnFilter((2<<16)+2), NewBitmapRowsFilter(rowSet))
	compareSlices(t, "column-rows", expected, rows)
	rows = getRows(t, filterSampleData, NewBitmapColumnFilter((2<<16)+2), NewBitmapRowsFilter(rowSet), NewBitmapRowLimitFilter(1))
	compareSlices(t, "limit", expected[:1], rows)
}

func TestRowsUnion(t *testing.T) {
	requireSampleData(t)
	rowSet := []uint64{7, 11}
	expected := []uint64{1<<16 + 1, 7<<16 + 7, 11<<16 + 11}
	u := NewBitmapRowsUnion(rowSet)
	iter, _ := filterSampleData.Containers.Iterator(0)
	err := ApplyFilterToIterator(u, iter)
	if err != nil {
		t.Fatalf("unexpected filter error: %v", err)
	}
	out := u.Results(0)
	compareSlices(t, "sevenEleven", out.Slice(), expected)

	// regression check (FB-1497)... make sure we're getting the
	// expected bits in a higher shard as well
	expected2 := make([]uint64, len(expected))
	for i, exp := range expected {
		expected2[i] = exp + 2*1<<shardwidth.Exponent
	}
	out2 := u.Results(2)
	compareSlices(t, "sevenEleven2", out2.Slice(), expected2)
}

func TestBitmapFilter(t *testing.T) {
	requireSampleData(t)
	bm := NewBitmap()
	expected := []uint64{}
	positions := make([]uint64, 0, 80)
	callback := func(pos uint64) error {
		positions = append(positions, pos)
		return nil
	}
	for i := 1; i < rowWidth; i++ {
		positions = positions[:0]
		expected = append(expected, filterSamplePositions[i]...)
		sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })
		_, _ = bm.Add(uint64((i << 16) + i))
		filter := NewBitmapBitmapFilter(bm, callback)
		err := applyFilter(t, filterSampleData, filter)
		if err != nil {
			t.Fatalf("unexpected error applying filter: %v", err)
		}
		compareSlices(t, fmt.Sprintf("stride-%d", i), expected, positions)
	}
}

func TestNewBitmapBitmapFilter_static(t *testing.T) {

	bm := NewBitmap(1<<16, 2<<16, 5<<16)

	positions := make([]uint64, 0, 80)
	callback := func(pos uint64) error {
		positions = append(positions, pos)
		return nil
	}
	bbfilt := NewBitmapBitmapFilter(bm, callback)

	// now verify nextOffsets with a slightly different algorithm

	containers := make([]*Container, rowWidth)
	algoExpectedNextOffsets := make([]uint64, rowWidth)

	iter, _ := bm.Containers.Iterator(0)
	last := uint64(0)
	first := uint64(0)
	count := 0
	for iter.Next() {
		k, v := iter.Value()
		// Coerce container key into the 0-rowWidth range we'll be
		// using to compare against containers within each row.
		k = k & keyMask

		// we only have one row in filterColumns, so we won't be overwriting anything.
		containers[k] = v

		last = k
		if count == 0 {
			first = k
		}
		count++
	}

	//                           last = 5; first = 1
	// bbfilt.containers: [ - 1 2 - - 5 - -... (all - to end) ]
	// nextOffsets:       [ 1 2 5 5 5 1 1 1...(all 1s to end) ] desired
	curLast := last
	for i := rowWidth - 1; i >= 0; i-- {
		if uint64(i) >= last {
			algoExpectedNextOffsets[i] = first
		} else {
			algoExpectedNextOffsets[i] = curLast
			if containers[i] != nil {
				curLast = uint64(i)
			}
		}
	}

	// compare observed and algoExpectedNextOffsets:
	if !reflect.DeepEqual(bbfilt.nextOffsets, algoExpectedNextOffsets) {
		t.Errorf("observed bbfilt.nextOffsets: %v, expected %v", bbfilt.nextOffsets, algoExpectedNextOffsets)
	}
}

func TestNewBitmapBitmapFilter_random(t *testing.T) {

	rand.Seed(1)
	for N := 0; N < 100; N++ {
		bm := NewBitmap()
		shardWidth := uint64(rowWidth << 16)
		_ = shardWidth
		for i := 0; i < N; i++ {
			_, _ = bm.AddN(rand.Uint64() % shardWidth)
		}

		positions := make([]uint64, 0, 80)
		callback := func(pos uint64) error {
			positions = append(positions, pos)
			return nil
		}
		bbfilt := NewBitmapBitmapFilter(bm, callback)

		// now verify nextOffsets with a slightly different algorithm

		containers := make([]*Container, rowWidth)
		algoExpectedNextOffsets := make([]uint64, rowWidth)

		iter, _ := bm.Containers.Iterator(0)
		last := uint64(0)
		first := uint64(0)
		count := 0
		for iter.Next() {
			k, v := iter.Value()
			// Coerce container key into the 0-rowWidth range we'll be
			// using to compare against containers within each row.
			k = k & keyMask

			// we only have one row in filterColumns, so we won't be overwriting anything.
			containers[k] = v

			last = k
			if count == 0 {
				first = k
			}
			count++
		}

		// small example
		//                           last = 5; first = 1
		// bbfilt.containers: [ - 1 2 - - 5 - -... (all - to end) ]
		// nextOffsets:       [ 1 2 5 5 5 1 1 1...(all 1s to end) ] desired
		curLast := last
		for i := rowWidth - 1; i >= 0; i-- {
			if uint64(i) >= last {
				algoExpectedNextOffsets[i] = first
			} else {
				algoExpectedNextOffsets[i] = curLast
				if containers[i] != nil {
					curLast = uint64(i)
				}
			}
		}

		// compare observed and algoExpectedNextOffsets:
		if !reflect.DeepEqual(bbfilt.nextOffsets, algoExpectedNextOffsets) {
			t.Errorf("observed bbfilt.nextOffsets: %v, expected %v", bbfilt.nextOffsets, algoExpectedNextOffsets)
		}
	}
}

func TestLimitFilter(t *testing.T) {
	f := NewBitmapRowLimitFilter(5)

	for i := FilterKey(0); i < 5; i++ {
		key := i << rowExponent
		res := f.ConsiderKey(key, 1)
		if res.NoKey == ^FilterKey(0) {
			t.Fatalf("limit filter ended early on iteration %d", i)
		}
		if res.YesKey <= key {
			t.Fatalf("limit filter should always include until done")
		}
	}
	res := f.ConsiderKey(FilterKey(5)<<rowExponent, 1)
	if res.NoKey != ^FilterKey(0) {
		t.Fatalf("limit filter should have thought it was done, reported last key %d", res.NoKey)
	}
}

func TestFilterWithRows(t *testing.T) {
	tests := []struct {
		rows     []uint64
		callWith []uint64
		expect   [][2]bool
	}{
		{
			rows:     []uint64{},
			callWith: []uint64{0},
			expect:   [][2]bool{{false, true}},
		},
		{
			rows:     []uint64{0},
			callWith: []uint64{0},
			expect:   [][2]bool{{true, true}},
		},
		{
			rows:     []uint64{1},
			callWith: []uint64{0, 2},
			expect:   [][2]bool{{false, false}, {false, true}},
		},
		{
			rows:     []uint64{0},
			callWith: []uint64{1, 2},
			expect:   [][2]bool{{false, true}, {false, true}},
		},
		{
			rows:     []uint64{3, 9},
			callWith: []uint64{1, 2, 3, 10},
			expect:   [][2]bool{{false, false}, {false, false}, {true, false}, {false, true}},
		},
		{
			rows:     []uint64{0, 1, 2},
			callWith: []uint64{0, 1, 2},
			expect:   [][2]bool{{true, false}, {true, false}, {true, true}},
		},
	}

	for num, test := range tests {
		t.Run(fmt.Sprintf("%d_%v_with_%v", num, test.rows, test.callWith), func(t *testing.T) {
			if len(test.callWith) != len(test.expect) {
				t.Fatalf("Badly specified test - must expect the same number of values as calls.")
			}
			f := NewBitmapRowsFilter(test.rows)
			for i, id := range test.callWith {
				key := FilterKey(id) << rowExponent
				res := f.ConsiderKey(key, 1)
				inc := res.YesKey > key
				done := res.NoKey == ^FilterKey(0)
				if inc != test.expect[i][0] || done != test.expect[i][1] {
					t.Logf("rows %d, calling row %d, result %#v\n", test.rows, id, res)
					t.Fatalf("Calling with %d\nexp: %v,%v\ngot: %v,%v", id, test.expect[i][0], test.expect[i][1], inc, done)
				}
			}
		})
	}
}

func TestMutexDupFilter(t *testing.T) {
	tests := []struct {
		pairs  [][2]uint64
		expect map[uint64][]uint64
	}{
		{
			pairs:  [][2]uint64{{0, 0}, {1, 0}, {0, 1}},
			expect: map[uint64][]uint64{0: {0, 1}},
		},
		{
			pairs:  [][2]uint64{{0, 0}, {1, 0}, {0, 1}, {0, 2}},
			expect: map[uint64][]uint64{0: {0, 1, 2}},
		},
	}
	for num, test := range tests {
		t.Run(fmt.Sprintf("case%d", num), func(t *testing.T) {
			b := NewSliceBitmap()
			for _, p := range test.pairs {
				v := (p[1] << shardwidth.Exponent) | p[0]
				b.DirectAdd(v)
			}
			dup := NewBitmapMutexDupFilter(0, true, 9)
			iter, _ := b.Containers.Iterator(0)
			err := ApplyFilterToIterator(dup, iter)
			if err != nil {
				t.Fatalf("applying filter: %v", err)
			}
			expected := test.expect
			got := dup.Report()
			if len(expected) != len(got) {
				t.Fatalf("expected %d entries in duplicate map, got %d", len(expected), len(got))
			}
			for k, v := range expected {
				gv := got[k]
				if len(v) != len(gv) {
					t.Fatalf("for id %d, expected %d (len %d), got %d (len %d)", k, v, len(v), gv, len(gv))
				}
				for j := range v {
					if gv[j] != v[j] {
						t.Fatalf("for id %d, expected %d, got %d", k, v[j], gv[j])
					}
				}
			}
		})
	}
}

const (
	shardWidth = 1 << shardwidth.Exponent
)

func TestGetNextFromIteratorEmpty(t *testing.T) {
	data := NewBitmap().Roaring()
	cit, err := NewContainerIterator(data)
	if err != nil {
		t.Fatalf("getting roaring iterator: %v", err)
	}

	key, cont := getNextFromIterator(cit)
	if key != KEY_DONE || cont != nil {
		t.Fatalf("expected iterator done, but got: %d, %v", key, cont.Slice())
	}

}

func TestGetNextFromIterator(t *testing.T) {
	data := NewBitmap(1, 7, 100000, 500000, shardWidth, shardWidth+3, shardWidth+containerWidth-1, shardWidth+containerWidth).Roaring()
	cit, err := NewContainerIterator(data)
	if err != nil {
		t.Fatalf("getting roaring iterator: %v", err)
	}

	expected := []struct {
		key   FilterKey
		slice []uint16
	}{
		{0, []uint16{1, 7}},
		{1, []uint16{100000 % containerWidth}},
		{500000 / containerWidth, []uint16{500000 % containerWidth}},
		{shardWidth / containerWidth, []uint16{0, 3, 65535}},
		{shardWidth/containerWidth + 1, []uint16{0}},
	}

	for i, exp := range expected {
		key, cont := getNextFromIterator(cit)
		if key != exp.key {
			t.Errorf("key mismatch at %d, got: %d, exp: %d", i, key, exp.key)
		}
		if !reflect.DeepEqual(cont.Slice(), exp.slice) {
			t.Fatalf("data misatch at %d, got: %v, exp: %v", i, cont.Slice(), exp.slice)
		}
	}
	key, cont := getNextFromIterator(cit)
	if key != KEY_DONE || cont != nil {
		t.Fatalf("expected iterator done, but got: %d, %v", key, cont.Slice())
	}

}

func TestClearColumnsAndSetRewriter(t *testing.T) {
	data := NewBitmap(1, 7, 100000, 500000,
		shardWidth, shardWidth+1, shardWidth+7, shardWidth+containerWidth-1, shardWidth+containerWidth,
		shardWidth*2+7, shardWidth*2+9).Roaring()
	clearIter, err := NewRepeatedRowIteratorFromBytes(data)
	if err != nil {
		t.Fatalf("getting clear iterator: %v", err)
	}
	setIter, err := NewContainerIterator(data)
	if err != nil {
		t.Fatalf("getting set iterator: %v", err)
	}

	rewriter, err := NewClearAndSetRewriter(clearIter, setIter)
	if err != nil {
		t.Fatalf("getting rewriter: %v", err)
	}

	expClearKeys := []FilterKey{
		0, 1, 500000 / containerWidth,
		shardWidth / containerWidth, shardWidth/containerWidth + 1, (shardWidth + 500000) / containerWidth,
		shardWidth * 2 / containerWidth, shardWidth*2/containerWidth + 1, (shardWidth*2 + 500000) / containerWidth,
	}
	for i, key := range expClearKeys {
		if rewriter.curClearKey != key {
			t.Errorf("unexpected clear key at %d, exp: %d, got: %d", i, key, rewriter.curClearKey)
		}
		rewriter.nextClear()
	}

	expSetKeys := []FilterKey{
		0, 1, 500000 / containerWidth,
		shardWidth / containerWidth, shardWidth/containerWidth + 1,
		shardWidth * 2 / containerWidth, KEY_DONE,
	}
	for i, key := range expSetKeys {
		if rewriter.curSetKey != key {
			t.Errorf("unexpected clear key at %d, exp: %d, got: %d", i, key, rewriter.curClearKey)
		}
		rewriter.nextSet()
	}

}
