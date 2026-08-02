package redisstore

import (
	"context"
	"testing"

	"github.com/akshajnad/relay/internal/redistest"
	"github.com/akshajnad/relay/internal/store"
	"github.com/akshajnad/relay/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T, cfg store.Config) store.Store {
		s := New(redistest.Client(t), cfg)
		if err := s.LoadScripts(context.Background()); err != nil {
			t.Fatal(err)
		}
		return s
	})
}
