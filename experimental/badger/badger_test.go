package badger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/nastro"
)

// helper: random bytes of length n
func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// helper: random hex string of byte length n
func randHex(n int) string {
	return hex.EncodeToString(randBytes(n))
}

// helper: construct a minimally valid event for badger with hex fields
func makeHexEvent() nostr.Event {
	return nostr.Event{
		ID:        randHex(32),                        // 32 bytes -> 64 hex chars
		PubKey:    randHex(32),                        // 32 bytes -> 64 hex chars
		CreatedAt: nostr.Timestamp(time.Now().Unix()), // use a reasonable timestamp
		Kind:      30000,
		Tags:      nostr.Tags{{"d", "test-tag"}},
		Content:   "test",
		Sig:       randHex(64), // 64 bytes -> 128 hex chars
	}
}

func TestSave(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	ev := makeHexEvent()
	if err := store.Save(ctx, &ev); err != nil {
		t.Fatal(err)
	}

	res, err := store.Query(ctx, nostr.Filter{IDs: []string{ev.ID}, Limit: 1})
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if len(res) != 1 {
		t.Fatalf("expected one event, got %v", res)
	}

	if !reflect.DeepEqual(res[0], ev) {
		t.Errorf("the event is not what it was before!")
		t.Fatalf(" expected %v\n got %v", ev, res[0])
	}
}

func makeReplaceableEventPair() (eventOld, eventNew nostr.Event) {
	// Base event with same ID, PubKey, etc but different CreatedAt
	baseID := randHex(32)
	basePubKey := randHex(32)
	baseSig := randHex(64)
	eventOld = nostr.Event{
		ID:        baseID,
		PubKey:    basePubKey,
		CreatedAt: 10,
		Kind:      30000,
		Tags:      nostr.Tags{{"d", "test-tag"}},
		Content:   "test old",
		Sig:       baseSig,
	}
	eventNew = nostr.Event{
		ID:        randHex(32), // different ID but same kind/pubkey/dtag should replace
		PubKey:    basePubKey,
		CreatedAt: 100,
		Kind:      30000,
		Tags:      nostr.Tags{{"d", "test-tag"}}, // same d tag
		Content:   "test new",
		Sig:       randHex(64),
	}
	return
}

func TestReplace(t *testing.T) {
	eventOld, eventNew := makeReplaceableEventPair()
	tests := []struct {
		name           string
		stored         nostr.Event
		newEvt         nostr.Event
		expectedStored bool
	}{
		{
			name:           "no replace (event is not newer)",
			stored:         eventNew,
			newEvt:         eventOld,
			expectedStored: false,
		},
		{
			name:           "valid replace (event is newer)",
			stored:         eventOld,
			newEvt:         eventNew,
			expectedStored: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := t.TempDir()
			store, err := New(ctx, path)
			if err != nil {
				t.Fatal(err)
			}

			if err := store.Save(ctx, &test.stored); err != nil {
				t.Fatal(err)
			}

			storedFlag, err := store.Replace(ctx, &test.newEvt)
			if err != nil {
				t.Fatal(err)
			}

			if storedFlag != test.expectedStored {
				t.Fatalf("expected stored %v, got %v", test.expectedStored, storedFlag)
			}

			res, err := store.Query(ctx, nostr.Filter{IDs: []string{test.stored.ID, test.newEvt.ID}, Limit: 1})
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if len(res) != 1 {
				t.Errorf("expected one event, got %d", len(res))
			}

			switch storedFlag {
			case true:
				if !reflect.DeepEqual(res[0], test.newEvt) {
					t.Fatalf("new was not saved correctly.\n original %v, got %v", test.newEvt, res[0])
				}

			case false:
				if !reflect.DeepEqual(res[0], test.stored) {
					t.Fatalf("stored has been altered.\n original %v, got %v", test.stored, res[0])
				}
			}
		})
	}
}

func TestInterface(t *testing.T) {
	var _ nastro.Store = &Store{}
}

// ---- Conversion round-trip tests ----

func TestEventConversionRoundTripJSON(t *testing.T) {
	// random go-nostr event
	ev1 := nostr.Event{
		ID:        randHex(32),
		PubKey:    randHex(32),
		CreatedAt: nostr.Timestamp(time.Now().Unix()),
		Kind:      1,
		Tags:      nostr.Tags{{"e", randHex(32)}, {"p", randHex(32)}, {"d", "topic"}},
		Content:   "hello world",
		Sig:       randHex(64),
	}

	orly1, err := GoNostrToOrly(&ev1)
	if err != nil {
		t.Fatalf("GoNostrToOrly failed: %v", err)
	}
	// back to go-nostr
	ev2, err := OrlyToGoNostr(orly1)
	if err != nil {
		t.Fatalf("OrlyToGoNostr failed: %v", err)
	}
	// and again to orly
	orly2, err := GoNostrToOrly(ev2)
	if err != nil {
		t.Fatalf("GoNostrToOrly(again) failed: %v", err)
	}

	b1 := orly1.Marshal(nil)
	b2 := orly2.Marshal(nil)
	if !reflect.DeepEqual(b1, b2) {
		t.Fatalf("orly event JSON mismatch after round-trip\n first:  %s\n second: %s", string(b1), string(b2))
	}
}

func randomFilter() nostr.Filter {
	f := nostr.Filter{}
	// randomize some fields
	f.IDs = []string{randHex(32), randHex(32)}
	f.Kinds = []int{1, 7, 30000}
	f.Authors = []string{randHex(32)}
	f.Tags = nostr.TagMap{
		"e": {randHex(32), randHex(32)},
		"p": {randHex(32)},
		"d": {"topic"},
	}
	since := nostr.Timestamp(time.Now().Add(-time.Hour).Unix())
	until := nostr.Timestamp(time.Now().Add(time.Hour).Unix())
	f.Since = &since
	f.Until = &until
	f.Search = "find me"
	f.Limit = 42
	return f
}

func TestFilterConversionRoundTripJSON(t *testing.T) {
	gf1 := randomFilter()

	orly1, err := GoNostrFilterToOrly(&gf1)
	if err != nil {
		t.Fatalf("GoNostrFilterToOrly failed: %v", err)
	}
	gf2 := OrlyFilterToGoNostr(orly1)
	orly2, err := GoNostrFilterToOrly(gf2)
	if err != nil {
		t.Fatalf("GoNostrFilterToOrly(again) failed: %v", err)
	}

	b1 := orly1.Marshal(nil)
	b2 := orly2.Marshal(nil)
	if !reflect.DeepEqual(b1, b2) {
		t.Fatalf("orly filter JSON mismatch after round-trip\n first:  %s\n second: %s", string(b1), string(b2))
	}
}
