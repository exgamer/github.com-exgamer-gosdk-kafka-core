package config

type HandlerRegister struct {
	Handler Handler
	Topic   string
	Config  []ConsumerOption
}
