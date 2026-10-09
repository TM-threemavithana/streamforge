package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	ingestv1 "github.com/example/streamforge/services/core-go/gen/streamforge/ingest/v1"
	"github.com/example/streamforge/services/core-go/internal/application"
	"github.com/example/streamforge/services/core-go/internal/eventstream"
	"github.com/example/streamforge/services/core-go/internal/ingestgrpc"
	postgresrepo "github.com/example/streamforge/services/core-go/internal/postgres"
	"github.com/example/streamforge/services/core-go/internal/restapi"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("core service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := os.Getenv("STREAMFORGE_DATABASE_URL")
	if databaseURL == "" {
		return errors.New("STREAMFORGE_DATABASE_URL is required")
	}
	grpcAddress := os.Getenv("STREAMFORGE_GRPC_ADDR")
	if grpcAddress == "" {
		grpcAddress = "127.0.0.1:50051"
	}
	httpAddress := os.Getenv("STREAMFORGE_HTTP_ADDR")
	if httpAddress == "" {
		httpAddress = "127.0.0.1:8080"
	}
	if err := requireLoopback("gRPC", grpcAddress); err != nil {
		return err
	}
	if err := requireLoopback("HTTP", httpAddress); err != nil {
		return err
	}

	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = database.PingContext(ctx); err != nil {
		return err
	}

	grpcListener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return err
	}
	httpListener, err := net.Listen("tcp", httpAddress)
	if err != nil {
		_ = grpcListener.Close()
		return err
	}

	eventRepository := application.EventRepository(postgresrepo.NewEventRepository(database))
	var restOptions []restapi.Option
	var kafkaClient *kgo.Client
	if brokers := commaSeparated(os.Getenv("STREAMFORGE_KAFKA_BROKERS")); len(brokers) > 0 {
		topic := os.Getenv("STREAMFORGE_KAFKA_TOPIC")
		if topic == "" {
			topic = "streamforge.raw-events.v1"
		}
		kafkaClient, err = kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ClientID("streamforge-ingestion-v1"),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.RecordRetries(3),
			kgo.RecordDeliveryTimeout(10*time.Second),
		)
		if err != nil {
			return err
		}
		defer kafkaClient.Close()
		kafkaContext, kafkaCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer kafkaCancel()
		if err = kafkaClient.Ping(kafkaContext); err != nil {
			return err
		}
		eventRepository = eventstream.NewRepository(topic, eventstream.NewKafkaPublisher(kafkaClient))
		consumerGroup := os.Getenv("STREAMFORGE_KAFKA_CONSUMER_GROUP")
		if consumerGroup == "" {
			consumerGroup = "streamforge-analytics-v1"
		}
		restOptions = append(restOptions, restapi.WithConsumerLag(eventstream.NewLagReader(kafkaClient, topic, consumerGroup)))
		slog.Info("Kafka ingestion enabled", "brokers", brokers, "topic", topic, "ack_stage", eventstream.KafkaPublished)
	}

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(ingestgrpc.MaxRequestBytes))
	ingestv1.RegisterTripIngestionServiceServer(grpcServer, ingestgrpc.NewServer(eventRepository))
	httpServer := &http.Server{
		Handler:           restapi.NewServer(postgresrepo.NewCatalogRepository(database), restOptions...).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsChannel := make(chan error, 2)
	go func() { errorsChannel <- grpcServer.Serve(grpcListener) }()
	go func() { errorsChannel <- httpServer.Serve(httpListener) }()
	slog.Info("StreamForge core service listening", "grpc_address", grpcListener.Addr().String(), "http_address", httpListener.Addr().String())

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errorsChannel:
	}
	shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownContext)
	grpcServer.GracefulStop()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

func commaSeparated(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

func requireLoopback(name, address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return errors.New(name + " address must be host:port")
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New(name + " address must use a loopback host until authenticated TLS is configured")
	}
	return nil
}
