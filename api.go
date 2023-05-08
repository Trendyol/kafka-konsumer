package kafka

import (
	"fmt"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type API interface {
	Start()
	Stop()
}

type server struct {
	fiber  *fiber.App
	cfg    *ConsumerConfig
	logger LoggerInterface
}

func NewAPI(cfg *ConsumerConfig) API {
	setDefaults(cfg)

	svr := server{
		cfg:    cfg,
		logger: NewZapLogger(cfg.LogLevel),
		fiber: fiber.New(
			fiber.Config{
				DisableStartupMessage:    true,
				DisableDefaultDate:       true,
				DisableHeaderNormalizing: true,
			},
		),
	}

	svr.fiber.Get(*cfg.MetricConfiguration.Path, adaptor.HTTPHandler(promhttp.Handler()))
	svr.fiber.Get(*cfg.APIConfiguration.HealthCheckPath, svr.HealthCheckHandler)

	return &svr
}

func setDefaults(cfg *ConsumerConfig) {
	if cfg.APIConfiguration.Port == nil {
		cfg.APIConfiguration.Port = intPointer(8090)
	}
	if cfg.APIConfiguration.HealthCheckPath == nil {
		cfg.APIConfiguration.HealthCheckPath = strPointer("/healthcheck")
	}
	if cfg.MetricConfiguration.Path == nil {
		cfg.MetricConfiguration.Path = strPointer("/metrics")
	}
}

func (s *server) Start() {
	s.logger.Infof("server starting on port %d", *s.cfg.APIConfiguration.Port)

	if err := s.fiber.Listen(fmt.Sprintf(":%d", *s.cfg.APIConfiguration.Port)); err != nil {
		s.logger.Errorf("server cannot start on port %d, err: %v", *s.cfg.APIConfiguration.Port, err)
	}
}

func (s *server) Stop() {
	s.logger.Info("server is closing")

	if err := s.fiber.Shutdown(); err != nil {
		s.logger.Errorf("server cannot be shutdown, err: %v", err)
		panic(err)
	}
}

func (s *server) HealthCheckHandler(c *fiber.Ctx) error {
	return c.SendStatus(fiber.StatusOK)
}

func strPointer(val string) *string {
	return &val
}

func intPointer(val int) *int {
	return &val
}
