package service_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/testutil"
)

func TestStartQuery_DrainingRejected(t *testing.T) {
	svc, lm := testutil.NewService(t)
	lm.SetState(lifecycle.StateDraining)

	_, err := svc.StartQuery(context.Background(), "testdb", "SELECT 1", domain.QueryOptions{Mode: "async"})
	if err == nil {
		t.Fatal("expected error when draining, got nil")
	}
}

func TestGetQueryStats(t *testing.T) {
	svc, _ := testutil.NewService(t)
	rec, err := svc.StartQuery(context.Background(), "testdb", "SELECT 1", domain.QueryOptions{Mode: "sync"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	stats, err := svc.GetQueryStats(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("GetQueryStats: %v", err)
	}
	if stats.RowsRead != 2 {
		t.Errorf("rows_read = %d, want 2", stats.RowsRead)
	}
}

func TestDownloadResult_Full(t *testing.T) {
	svc, _ := testutil.NewService(t)
	rec, err := svc.StartQuery(context.Background(), "testdb", "SELECT id, name FROM t",
		domain.QueryOptions{Mode: "sync", ResultFormat: "jsonl"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}

	reader, ref, err := svc.DownloadResult(context.Background(), rec.ID, 0, 0)
	if err != nil {
		t.Fatalf("DownloadResult: %v", err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if len(data) == 0 {
		t.Fatal("empty result body")
	}
	if ref.RowCount != 2 {
		t.Errorf("row_count = %d, want 2", ref.RowCount)
	}
}

func TestDownloadResult_OffsetLimit(t *testing.T) {
	svc, _ := testutil.NewService(t)
	rec, err := svc.StartQuery(context.Background(), "testdb", "SELECT id, name FROM t",
		domain.QueryOptions{Mode: "sync", ResultFormat: "jsonl"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}

	// Full body for reference.
	full, _, err := svc.DownloadResult(context.Background(), rec.ID, 0, 0)
	if err != nil {
		t.Fatalf("DownloadResult full: %v", err)
	}
	fullData, _ := io.ReadAll(full)
	full.Close()

	// offset=2, limit=5 → bytes [2,7) of the full body.
	part, _, err := svc.DownloadResult(context.Background(), rec.ID, 2, 5)
	if err != nil {
		t.Fatalf("DownloadResult partial: %v", err)
	}
	partData, _ := io.ReadAll(part)
	part.Close()

	if len(partData) != 5 {
		t.Fatalf("partial length = %d, want 5", len(partData))
	}
	if string(partData) != string(fullData[2:7]) {
		t.Errorf("partial bytes = %q, want %q", partData, fullData[2:7])
	}
}

func TestDownloadResult_NotSucceeded(t *testing.T) {
	svc, _ := testutil.NewService(t)
	// streamdb stays RUNNING; download must be rejected.
	rec, err := svc.StartQuery(context.Background(), "slowdb", "SELECT *", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	// Give it a moment to enter RUNNING.
	time.Sleep(100 * time.Millisecond)
	if _, _, err := svc.DownloadResult(context.Background(), rec.ID, 0, 0); err == nil {
		t.Error("expected error downloading a non-succeeded query")
	}
}

func TestListDatabases(t *testing.T) {
	svc, _ := testutil.NewService(t)
	dbs, err := svc.ListDatabases(context.Background())
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(dbs) != 2 {
		t.Fatalf("got %d databases, want 2", len(dbs))
	}
	byID := map[string]domain.DatabaseInfo{}
	for _, d := range dbs {
		byID[d.ID] = d
	}
	if _, ok := byID["testdb"]; !ok {
		t.Error("missing testdb")
	}
	if !byID["testdb"].Healthy {
		t.Error("testdb should be healthy (fake ping ok)")
	}
}

func TestCanIBeStopped(t *testing.T) {
	svc, _ := testutil.NewService(t)
	canStop, inFlight := svc.CanIBeStopped(context.Background())
	if !canStop || inFlight != 0 {
		t.Errorf("can_stop=%v in_flight=%d, want true/0", canStop, inFlight)
	}
}
