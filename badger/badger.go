// Package badger implements a badger DB based event store for nostr.
package badger

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/nastro"
	"github.com/templexxx/xhex"
	"lol.mleku.dev/chk"
	"next.orly.dev/pkg/crypto/ec/schnorr"
	"next.orly.dev/pkg/crypto/sha256"
	"next.orly.dev/pkg/database"
	"next.orly.dev/pkg/encoders/event"
	"next.orly.dev/pkg/encoders/filter"
	"next.orly.dev/pkg/encoders/kind"
	"next.orly.dev/pkg/encoders/tag"
	"next.orly.dev/pkg/encoders/timestamp"
	"next.orly.dev/pkg/utils/values"
)

type Store struct {
	*database.D
	validateEvent   nastro.EventPolicy
	sanitizeFilters nastro.FilterPolicy
}

type Option func(*Store) error

// WithFilterPolicy sets a custom [nastro.FilterPolicy] on the Store.
// It will be used to validate and modify filters before executing queries.
func WithFilterPolicy(v nastro.FilterPolicy) Option {
	return func(s *Store) error {
		s.sanitizeFilters = v
		return nil
	}
}

// WithEventPolicy sets a custom [nastro.EventPolicy] on the Store.
// It will be used to validate events before inserting them into the database.
func WithEventPolicy(v nastro.EventPolicy) Option {
	return func(s *Store) error {
		s.validateEvent = v
		return nil
	}
}

// New returns a badger-based store located at the provided path,
// after applying the provided options.
func New(ctx context.Context, path string, opts ...Option) (
	s *Store, err error,
) {
	var db *database.D
	if db, err = database.New(ctx, func() {}, path, "info"); chk.E(err) {
		return
	}
	s = &Store{
		D:               db,
		validateEvent:   func(*nostr.Event) error { return nil },
		sanitizeFilters: nastro.DefaultFilterPolicy,
	}

	for _, opt := range opts {
		if err = opt(s); err != nil {
			s = nil
			return
		}
	}
	// close the store when the context is cancelled
	go func() {
		<-ctx.Done()
		chk.E(s.Close())
	}()
	return
}

func GoNostrToOrly(ev *nostr.Event) (
	orly *event.E, err error,
) {
	if ev == nil {
		err = errors.New("event is nil")
		return
	}
	// decode the id, pubkey, and sig from hex strings
	id := make([]byte, sha256.Size)
	pk := make([]byte, schnorr.PubKeyBytesLen)
	sig := make([]byte, schnorr.SignatureSize)
	if err = xhex.Decode(id, []byte(ev.ID)); chk.E(err) {
		orly = nil
		return
	}
	if err = xhex.Decode(pk, []byte(ev.PubKey)); chk.E(err) {
		orly = nil
		return
	}
	if err = xhex.Decode(sig, []byte(ev.Sig)); chk.E(err) {
		orly = nil
		return
	}
	// convert the tags to the type required for orly.Event
	ts := make([]*tag.T, 0, len(ev.Tags))
	for i := range ev.Tags {
		var t [][]byte
		for j := range ev.Tags[i] {
			t = append(t, []byte(ev.Tags[i][j]))
		}
		ts = append(ts, tag.NewFromBytesSlice(t...))
	}
	orly = &event.E{
		ID:        id,
		Pubkey:    pk,
		CreatedAt: int64(ev.CreatedAt),
		Kind:      uint16(ev.Kind),
		Content:   []byte(ev.Content),
		Tags:      tag.NewS(ts...),
		Sig:       sig,
	}
	return
}

func OrlyToGoNostr(orly *event.E) (ev *nostr.Event, err error) {
	// encode the id, pubkey, and sig as hex strings
	id := make([]byte, sha256.Size*2)
	xhex.Encode(id, orly.ID)
	pk := make([]byte, schnorr.PubKeyBytesLen*2)
	xhex.Encode(pk, orly.Pubkey)
	sig := make([]byte, schnorr.SignatureSize*2)
	xhex.Encode(sig, orly.Sig)
	// convert the tags to the type required for nostr.Event
	ts := orly.Tags.ToSliceOfSliceOfStrings()
	tags := make([]nostr.Tag, len(ts))
	for i := range ts {
		tags[i] = make(nostr.Tag, len(ts[i]))
		copy(tags[i], ts[i])
	}
	ev = &nostr.Event{
		ID:        string(id),
		PubKey:    string(pk),
		CreatedAt: nostr.Timestamp(orly.CreatedAt),
		Kind:      int(orly.Kind),
		Tags:      tags,
		Content:   string(orly.Content),
		Sig:       string(sig),
	}
	return
}

func (s *Store) Save(ctx context.Context, ev *nostr.Event) (err error) {
	if err = s.validateEvent(ev); err != nil {
		return
	}
	var evo *event.E
	if evo, err = GoNostrToOrly(ev); chk.E(err) {
		return
	}
	if _, err = s.D.SaveEvent(ctx, evo); chk.E(err) {
		return
	}
	return
}

func (s *Store) Delete(ctx context.Context, id string) (err error) {
	idBytes := make([]byte, sha256.Size)
	if err = xhex.Decode(idBytes, []byte(id)); err != nil {
		return
	}
	return s.DeleteEvent(ctx, idBytes)
}

func (s *Store) Replace(ctx context.Context, ev *nostr.Event) (
	replaced bool, err error,
) {
	if !(kind.IsReplaceable(uint16(ev.Kind)) ||
		kind.IsParameterizedReplaceable(uint16(ev.Kind))) {
		err = nastro.ErrInvalidReplacement
		return
	}
	var evo *event.E
	if evo, err = GoNostrToOrly(ev); err != nil {
		return
	}
	// save the event (if it replaces, it replaces)
	if replaced, err = s.D.SaveEvent(ctx, evo); err != nil {
		// If the error indicates the event is blocked (older than existing),
		// return false without error as this is expected behavior
		if strings.Contains(err.Error(), "blocked:") {
			err = nil
			replaced = false
			return
		}
		return
	}
	return
}

// Query executes multiple nostr filters concurrently and returns matching
// events. When multiple filters are provided, the results are concatenated in a
// non-deterministic order due to concurrent execution. Each filter is processed
// in a separate goroutine, and the final event slice contains events from all
// filters without any guaranteed ordering between filters' results. The method
// returns an error if any filter query fails. The results of multiple filters,
// the individual result groups are still in the same order as the individual
// filter produced.
func (s *Store) Query(
	ctx context.Context, filters ...nostr.Filter,
) (evs []nostr.Event, err error) {
	// Simple non-concurrent version to debug
	var oevs event.S
	for _, filter := range filters {
		ff, err := GoNostrFilterToOrly(&filter)
		if err != nil {
			return nil, err
		}
		var es event.S
		if es, err = s.QueryEvents(ctx, ff); err != nil {
			return nil, err
		}
		oevs = append(oevs, es...)
	}
	for _, ev := range oevs {
		var oe *nostr.Event
		if oe, err = OrlyToGoNostr(ev); err != nil {
			return nil, err
		}
		evs = append(evs, *oe)
	}
	return evs, nil
}

// Count returns the number of events matching the provided filters. If multiple
// filters are provided, the results are concatenated in a non-deterministic
// order due to concurrent execution. Each filter is processed in a separate
// goroutine, and the final count is the sum of the individual filter counts.
// The method returns an error if any filter query fails. The results of
// multiple filters, the individual result groups are still in the same order as
// the individual filter produced. Note that it can return a count and an error
// at the same time, with multiple filters.
func (s *Store) Count(ctx context.Context, filters ...nostr.Filter) (
	count int64, err error,
) {
	var counter atomic.Int64
	var wg sync.WaitGroup
	for _, f := range filters {
		wg.Add(1)
		go func(filter nostr.Filter) {
			defer wg.Done()
			ff, err := GoNostrFilterToOrly(&filter)
			if err != nil {
				return
			}
			var c int
			if c, _, err = s.CountEvents(ctx, ff); err != nil {
				return
			}
			counter.Add(int64(c))
		}(f)
	}
	wg.Wait()
	count = counter.Load()
	return
}

func GoNostrFilterToOrly(gf *nostr.Filter) (f *filter.F, err error) {
	f = &filter.F{}
	var ids [][]byte
	for _, id := range gf.IDs {
		idb := make([]byte, sha256.Size)
		if err = xhex.Decode(idb, []byte(id)); err != nil {
			return
		}
		ids = append(ids, idb)
	}
	f.Ids = tag.NewFromBytesSlice(ids...)
	var kinds kind.S
	for _, k := range gf.Kinds {
		kinds.K = append(kinds.K, &kind.K{K: uint16(k)})
	}
	f.Kinds = &kinds
	var authors [][]byte
	for _, pk := range gf.Authors {
		pkb := make([]byte, schnorr.PubKeyBytesLen)
		if err = xhex.Decode(pkb, []byte(pk)); err != nil {
			return
		}
		authors = append(authors, pkb)
	}
	f.Authors = tag.NewFromBytesSlice(authors...)
	var ts []*tag.T
	for key, val := range gf.Tags {
		var tbs [][]byte
		tbs = append(tbs, []byte(key))
		for _, v := range val {
			tbs = append(tbs, []byte(v))
		}
		t := tag.NewFromBytesSlice(tbs...)
		ts = append(ts, t)
	}
	f.Tags = tag.NewS(ts...)
	if gf.Since != nil {
		f.Since = timestamp.FromUnix(gf.Since.Time().Unix())
	}
	if gf.Until != nil {
		f.Until = timestamp.FromUnix(gf.Until.Time().Unix())
	}
	if gf.Search != "" {
		f.Search = []byte(gf.Search)
	}
	if gf.Limit > 0 && !gf.LimitZero {
		f.Limit = values.ToUintPointer(uint(gf.Limit))
	}
	return
}

func OrlyFilterToGoNostr(f *filter.F) (gf *nostr.Filter) {
	gf = &nostr.Filter{}
	for _, id := range f.Ids.T {
		idh := make([]byte, sha256.Size*2)
		xhex.Encode(idh, id)
		gf.IDs = append(gf.IDs, string(idh))
	}
	for _, k := range f.Kinds.K {
		gf.Kinds = append(gf.Kinds, int(k.K))
	}
	for _, aut := range f.Authors.T {
		idh := make([]byte, schnorr.PubKeyBytesLen*2)
		xhex.Encode(idh, aut)
		gf.Authors = append(gf.Authors, string(idh))
	}
	gf.Tags = make(nostr.TagMap)
	if f.Tags != nil {
		for _, t := range f.Tags.ToSliceOfSliceOfStrings() {
			if len(t) > 0 {
				gf.Tags[t[0]] = t[1:]
			}
		}
	}
	if f.Since != nil {
		ts := nostr.Timestamp(f.Since.I64())
		gf.Since = &ts
	}
	if f.Until != nil {
		ts := nostr.Timestamp(f.Until.I64())
		gf.Until = &ts
	}
	if len(f.Search) > 0 {
		gf.Search = string(f.Search)
	}
	if f.Limit != nil {
		gf.Limit = int(*f.Limit)
		if gf.Limit == 0 {
			gf.LimitZero = true
		}
	}
	return
}
