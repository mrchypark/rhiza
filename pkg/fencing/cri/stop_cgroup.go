package cri

type stopCgroupPin interface {
	verifyEmpty() error
	close()
}
