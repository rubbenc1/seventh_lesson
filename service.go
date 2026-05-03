package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type ACLManager struct {
	rules map[string][]string
	admin *AdminManager
}

func (m *ACLManager) checkAccess(consumer, method string) bool {
	patterns, ok := m.rules[consumer]
	if !ok {
		return false
	}

	for _, pattern := range patterns {
		if strings.HasSuffix(pattern, "*") {
			prefix := strings.TrimSuffix(pattern, "*")
			if strings.HasPrefix(method, prefix) {
				return true
			}
		} else {
			if pattern == method {
				return true
			}
		}
	}
	return false
}

func (m *ACLManager) aclInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "Metadata is missing")
	}
	values := md.Get("consumer")
	if len(values) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing consumer")
	}
	p, _ := peer.FromContext(ctx)
	if !m.checkAccess(values[0], info.FullMethod) {
		return nil, status.Error(codes.Unauthenticated, "access denied")
	}
	m.admin.Publish(&Event{
		Timestamp: time.Now().Unix(),
		Consumer:  values[0],
		Method:    info.FullMethod,
		Host:      p.Addr.String(),
	})
	return handler(ctx, req)
}

func (m *ACLManager) streamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return status.Errorf(codes.Unauthenticated, "Metadata is missing")
	}
	values := md.Get("consumer")
	if len(values) == 0 {
		return status.Error(codes.Unauthenticated, "missing consumer")
	}
	p, _ := peer.FromContext(ss.Context())
	if !m.checkAccess(values[0], info.FullMethod) {
		return status.Error(codes.Unauthenticated, "access denied")
	}
	m.admin.Publish(&Event{
		Timestamp: time.Now().Unix(),
		Consumer:  values[0],
		Method:    info.FullMethod,
		Host:      p.Addr.String(),
	})
	return handler(srv, ss)
}

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
func StartMyMicroservice(ctx context.Context, listenAddr, ACLData string) error {

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	needClose := true
	defer func() {
		if needClose {
			lis.Close()
		}
	}()
	rules := make(map[string][]string)
	if err := json.Unmarshal([]byte(ACLData), &rules); err != nil {
		return err
	}
	adminSrv := NewAdminManager()
	acl := &ACLManager{
		rules: rules,
		admin: adminSrv,
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(acl.aclInterceptor),
		grpc.StreamInterceptor(acl.streamInterceptor),
	)

	RegisterBizServer(server, NewBizManager())
	RegisterAdminServer(server, adminSrv)

	// 1. Запускаем сервер в фоне
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Printf("Server stopped: %v", err)
		}
	}()

	// 2. Ждем сигнала остановки в фоне
	go func() {
		<-ctx.Done()
		server.GracefulStop()
		lis.Close()
	}()
	needClose = false
	return nil
}
