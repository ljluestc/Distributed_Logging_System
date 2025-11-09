package config

import "fmt"

type Config struct {
	HostIP        string
	KafkaPort     string
	FluentdPort   int
	KafkaBrokers  []string
	FluentdHost   string
	BootstrapAddr string
}

func Default() Config {
	host := "192.168.222.127"
	kafkaPort := "9092"
	fluentPort := 24224
	return Config{
		HostIP:        host,
		KafkaPort:     kafkaPort,
		FluentdPort:   fluentPort,
		KafkaBrokers:  []string{fmt.Sprintf("%s:%s", host, kafkaPort)},
		FluentdHost:   host,
		BootstrapAddr: fmt.Sprintf("%s:%s", host, kafkaPort),
	}
}



