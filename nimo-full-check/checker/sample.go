package checker

import "math/rand"

const (
	SEED = 1
)

type Sample struct {
	sampleCnt int
	totalCnt  int
	source    *rand.Rand
}

func NewSample(sampleCnt, totalCnt int) *Sample {
	return &Sample{
		sampleCnt: sampleCnt,
		totalCnt:  totalCnt,
		source:    rand.New(rand.NewSource(SEED)),
	}
}

func (s *Sample) Hit() bool {
	if s.sampleCnt >= s.totalCnt {
		return true
	}
	if s.sampleCnt == 0 {
		return false
	}

	return s.source.Intn(s.totalCnt) < s.sampleCnt
}
