package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ACLManager struct {
	rules map[string][]string
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


func (m *ACLManager)atlInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	md, ok:=metadata.FromIncomingContext(ctx)
	if !ok {
        return nil, status.Errorf(codes.Unauthenticated, "Metadata is missing")
    }
    values := md.Get("consumer")
    if len(values) == 0 {
        return nil, status.Error(codes.Unauthenticated, "missing consumer")
    }
	if !m.checkAccess(values[0],info.FullMethod) {
		return nil, status.Error(codes.Unauthenticated, "access denied")
	}
	return handler(ctx, req)
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
	rules:=make(map[string][]string)
	if err:=json.Unmarshal([]byte(ACLData),&rules); err!=nil {
		return err
	}
	acl:=&ACLManager{rules: rules}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(acl.atlInterceptor),
	)

	RegisterBizServer(server, NewBizManager())

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
	needClose=false
	return nil
}
