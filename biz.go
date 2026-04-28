package main

import "context"

type BizManager struct {
	UnimplementedBizServer
}

func NewBizManager() *BizManager {
	return &BizManager{}
}

func (bm *BizManager) Check(ctx context.Context, n *Nothing) (*Nothing, error) {
    return &Nothing{}, nil
}

func (bm *BizManager) Add(ctx context.Context, n *Nothing) (*Nothing, error) {
    return &Nothing{}, nil
}

func (bm *BizManager) Test(ctx context.Context, n *Nothing) (*Nothing, error) {
    return &Nothing{}, nil
}
