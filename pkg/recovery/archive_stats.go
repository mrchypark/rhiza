package recovery

import (
	"context"
	"sync"
	"time"
)

const (
	ArchiveStatsSchemaVersion uint64 = 1
	maxExactJSONInteger       uint64 = 1<<53 - 1
)

// ArchiveStageStats is one fixed archive-phase aggregate. A nil value is
// unavailable, including after an accumulator overflows the exact JSON range.
type ArchiveStageStats struct {
	Available     bool    `json:"available"`
	Count         *uint64 `json:"count"`
	DurationNSSum *uint64 `json:"duration_ns_sum"`
}

// ArchiveStages is deliberately a fixed shape; stage names are not dynamic.
type ArchiveStages struct {
	PublicationAdmission ArchiveStageStats `json:"publication_admission"`
	ArchiveLoad          ArchiveStageStats `json:"archive_load"`
	ExtentBuildUpload    ArchiveStageStats `json:"extent_build_upload"`
	HeadPublish          ArchiveStageStats `json:"head_publish"`
	PublicationRelease   ArchiveStageStats `json:"publication_release"`
}

// ArchiveStats is a versioned, process-local snapshot of archive phase totals.
type ArchiveStats struct {
	SchemaVersion uint64        `json:"schema_version"`
	Stages        ArchiveStages `json:"stages"`
}

type archiveDurationStage uint8

const (
	archivePublicationAdmission archiveDurationStage = iota
	archiveLoad
	archiveExtentBuildUpload
	archiveHeadPublish
	archivePublicationRelease
	archiveDurationStageCount
)

type archiveDurationAccumulator struct {
	count         uint64
	durationNSSum uint64
	invalid       bool
}

type archiveDurationStats struct {
	mu     sync.Mutex
	stages [archiveDurationStageCount]archiveDurationAccumulator
}

func (s *archiveDurationStats) record(stage archiveDurationStage, duration time.Duration) {
	if stage >= archiveDurationStageCount {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	acc := &s.stages[stage]
	if acc.count < maxExactJSONInteger {
		acc.count++
	} else {
		acc.count = maxExactJSONInteger
		acc.invalid = true
	}

	if duration < 0 {
		acc.invalid = true
		duration = 0
	}
	nanos := uint64(duration.Nanoseconds())
	if nanos > maxExactJSONInteger || acc.durationNSSum > maxExactJSONInteger-nanos {
		acc.durationNSSum = maxExactJSONInteger
		acc.invalid = true
	} else {
		acc.durationNSSum += nanos
	}
}

func (m *Manager) loadForSync(ctx context.Context) error {
	started := time.Now()
	err := m.Load(ctx)
	m.archiveStats.record(archiveLoad, time.Since(started))
	return err
}

func (s *archiveDurationStats) snapshot() ArchiveStats {
	if s == nil {
		return unavailableArchiveStats()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return ArchiveStats{
		SchemaVersion: ArchiveStatsSchemaVersion,
		Stages: ArchiveStages{
			PublicationAdmission: archiveStageSnapshot(s.stages[archivePublicationAdmission]),
			ArchiveLoad:          archiveStageSnapshot(s.stages[archiveLoad]),
			ExtentBuildUpload:    archiveStageSnapshot(s.stages[archiveExtentBuildUpload]),
			HeadPublish:          archiveStageSnapshot(s.stages[archiveHeadPublish]),
			PublicationRelease:   archiveStageSnapshot(s.stages[archivePublicationRelease]),
		},
	}
}

func archiveStageSnapshot(acc archiveDurationAccumulator) ArchiveStageStats {
	if acc.invalid {
		return ArchiveStageStats{}
	}
	count, duration := acc.count, acc.durationNSSum
	return ArchiveStageStats{Available: true, Count: &count, DurationNSSum: &duration}
}

func unavailableArchiveStats() ArchiveStats {
	return ArchiveStats{
		SchemaVersion: ArchiveStatsSchemaVersion,
		Stages: ArchiveStages{
			PublicationAdmission: ArchiveStageStats{},
			ArchiveLoad:          ArchiveStageStats{},
			ExtentBuildUpload:    ArchiveStageStats{},
			HeadPublish:          ArchiveStageStats{},
			PublicationRelease:   ArchiveStageStats{},
		},
	}
}
