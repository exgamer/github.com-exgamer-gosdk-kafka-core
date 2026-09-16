package config

func NewPublisherDefinition(name string, cfg ...PublisherOption) PublisherDefinition {
	return PublisherDefinition{
		Name:   name,
		Config: cfg,
	}
}

type PublisherDefinition struct {
	Name   string
	Config []PublisherOption
}
