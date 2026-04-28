package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
func StartMyMicroservice(ctx context.Context, listenAddr, ACLData string) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	server := grpc.NewServer()

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
	}()

	return nil
}
